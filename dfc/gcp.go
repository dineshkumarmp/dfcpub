// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.  *
 */
package dfc

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

//======
//
// implements cloudif
//
//======
type gcpimpl struct {
	t *targetrunner
}

//======
//
// global - FIXME: environ
//
//======
func getProjID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func gcpErrorToHTTP(gcpError error) int {
	if gcperror, ok := gcpError.(*googleapi.Error); ok {
		return gcperror.Code
	}

	return http.StatusInternalServerError
}

//======
//
// methods
//
//======
func (gcpimpl *gcpimpl) listbucket(bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int) {
	glog.Infof("gcp listbucket %s", bucket)
	client, gcpctx, errstr := createclient()
	if errstr != "" {
		return
	}

	var query *storage.Query = nil
	if msg.GetPrefix != "" {
		query = &storage.Query{Prefix: msg.GetPrefix}
	}
	it := client.Bucket(bucket).Objects(gcpctx, query)

	var reslist = BucketList{Entries: make([]*BucketEntry, 0, 1000)}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			errcode = gcpErrorToHTTP(err)
			errstr = fmt.Sprintf("gcp: Failed to list objects of bucket %s, err: %v", bucket, err)
		}
		entry := &BucketEntry{}
		entry.Name = attrs.Name
		if strings.Contains(msg.GetProps, GetPropsSize) {
			entry.Size = attrs.Size
		}
		if strings.Contains(msg.GetProps, GetPropsBucket) {
			entry.Bucket = attrs.Bucket
		}
		if strings.Contains(msg.GetProps, GetPropsCtime) {
			t := attrs.Created
			switch msg.GetTimeFormat {
			case "":
				fallthrough
			case RFC822:
				entry.Ctime = t.Format(time.RFC822)
			default:
				entry.Ctime = t.Format(msg.GetTimeFormat)
			}
		}
		if strings.Contains(msg.GetProps, GetPropsChecksum) {
			entry.Checksum = hex.EncodeToString(attrs.MD5)
		}
		// TODO: other GetMsg props TBD

		reslist.Entries = append(reslist.Entries, entry)
	}
	if glog.V(3) {
		glog.Infof("listbucket count %d", len(reslist.Entries))
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	return
}

// Initialize and create storage client
func createclient() (*storage.Client, context.Context, string) {
	if getProjID() == "" {
		return nil, nil, "Failed to get ProjectID from GCP"
	}
	gcpctx := context.Background()
	client, err := storage.NewClient(gcpctx)
	if err != nil {
		return nil, nil, fmt.Sprintf("Failed to create client, err: %v", err)
	}
	return client, gcpctx, ""
}

// FIXME: revisit error processing
func (gcpimpl *gcpimpl) getobj(fqn string, bucket string, objname string) (md5hash string, size int64, errstr string, errcode int) {
	client, gcpctx, errstr := createclient()
	if errstr != "" {
		return
	}
	objhdl := client.Bucket(bucket).Object(objname)
	attrs, err := objhdl.Attrs(gcpctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to get attributes (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	omd5 := hex.EncodeToString(attrs.MD5)

	// FIXME: 'if' block to debug parallel-read
	if true {
		md5hash, size, errstr = gcpimpl.readParallel(gcpctx, objhdl, attrs.Size)
		if errstr != "" {
			glog.Infof("Error: parallel-read (object %s, bucket %s): %s", objname, bucket, errstr)
		} else if omd5 == md5hash {
			glog.Infof("parallel-read (object %s, bucket %s): md5 OK", objname, bucket)
		} else {
			glog.Infof("Error: parallel-read (object %s, bucket %s): md5 mismatch (%s != %s)",
				objname, bucket, omd5[:8], md5hash[:8])
		}
	}

	gcpreader, err := objhdl.NewReader(gcpctx)
	if err != nil {
		errstr = fmt.Sprintf("gcp: Failed to create gcp reader (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	defer gcpreader.Close()
	if md5hash, size, errstr = gcpimpl.t.receiveFileAndFinalize(fqn, objname, omd5, gcpreader); errstr != "" {
		return
	}
	stats := getstorstats()
	stats.add("bytesloaded", size)
	if glog.V(3) {
		glog.Infof("gcp: GET %s (bucket %s)", objname, bucket)
	}
	return
}

func (gcpimpl *gcpimpl) putobj(file *os.File, bucket, objname string) (errstr string, errcode int) {
	client, gcpctx, errstr := createclient()
	if errstr != "" {
		return
	}
	wc := client.Bucket(bucket).Object(objname).NewWriter(gcpctx)
	buf := gcpimpl.t.buffers.alloc()
	defer gcpimpl.t.buffers.free(buf)
	written, err := io.CopyBuffer(wc, file, buf)
	if err != nil {
		errstr = fmt.Sprintf("gcp: Failed to copy-buffer (object %s, bucket %s), err: %v", objname, bucket, err)
		return
	}
	if err := wc.Close(); err != nil {
		errstr = fmt.Sprintf("gcp: Unexpected failure to close wc upon uploading %s (bucket %s), err: %v",
			objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("gcp: PUT %s (bucket %s, size %d) ", objname, bucket, written)
	}
	return
}

func (gcpimpl *gcpimpl) deleteobj(bucket, objname string) (errstr string, errcode int) {
	client, gcpctx, errstr := createclient()
	if errstr != "" {
		return
	}
	objhdl := client.Bucket(bucket).Object(objname)
	err := objhdl.Delete(gcpctx)
	if err != nil {
		errcode = gcpErrorToHTTP(err)
		errstr = fmt.Sprintf("gcp: Failed to delete %s (bucket %s), err: %v", objname, bucket, err)
		return
	}
	if glog.V(3) {
		glog.Infof("gcp: deleted %s (bucket %s)", objname, bucket)
	}
	return
}

//==============================================================================
//
//
//
//==============================================================================
type bufIO struct {
	offset int64
	buf    []byte
	read   int // FIXME: make it atomic
}

func (gcpimpl *gcpimpl) readParallel(gcpctx context.Context,
	objhdl *storage.ObjectHandle, osize int64) (md5hash string, size int64, errstr string) {
	// declare
	var (
		buffs      buffif
		bsize      int64
		inflight   int
		woff, roff int64
		npar       = 16                               // num goroutine readers
		rq         = make(chan *bufIO, npar)          // receive queue
		cq         = make(chan *bufIO, npar)          // completion queue
		eq         = make(chan error, npar)           // error queue
		lenring    = npar * 2                         // num reusable buffers and size of the ring
		ring       = make([]*bufIO, lenring, lenring) // the ring
		md5        = md5.New()                        // FIXME: debug only
	)
	if osize > gcpimpl.t.buffers32k.fixedsize {
		buffs = gcpimpl.t.buffers
		bsize = gcpimpl.t.buffers.fixedsize
	} else if osize > gcpimpl.t.buffers4k.fixedsize {
		buffs = gcpimpl.t.buffers32k
		bsize = gcpimpl.t.buffers32k.fixedsize
	} else {
		buffs = gcpimpl.t.buffers4k
		bsize = gcpimpl.t.buffers4k.fixedsize
	}
	// preallocate buffers
	for i := 0; i < lenring; i++ {
		bio := &bufIO{offset: roff, buf: buffs.alloc()}
		ring[i] = bio
	}
	// go
	for i := 0; i < npar; i++ {
		go gcpimpl.readRange(gcpctx, objhdl, rq, cq, eq, osize)
	}
	// feed initial batch
	for i := 0; i < npar && roff < osize; i++ {
		bio := ring[i]
		bio.read = 0
		rq <- bio
		inflight++
		roff += bsize
	}
	tick := time.Now() // FIXME: remove
loop: // work
	for woff < osize {
		select {
		case <-cq: // handle completions
			inflight--
			wpos := int(woff/bsize) % lenring
			for { // write
				wbio := ring[wpos]
				if wbio.read == 0 {
					break
				}
				md5.Write(wbio.buf[:wbio.read])
				woff += int64(wbio.read)
				assert(int64(wbio.read) == bsize || woff == osize, fmt.Sprintf("%d, %d, %d", wbio.read, woff, osize))
				wbio.read = 0
				if woff >= osize {
					break loop
				}
				wpos++
				if wpos >= lenring {
					wpos = 0
				}
			}
			for inflight < npar && roff < osize { // replenish
				rpos := int(roff/bsize) % lenring
				rbio := ring[rpos]
				rbio.offset = roff
				rbio.read = 0
				rq <- rbio
				inflight++
				roff += bsize
			}
		case err := <-eq:
			errstr = fmt.Sprintf("gcp rr: %v", err)
			break loop
		default: // FIXME
			if time.Now().After(tick) {
				glog.Infoln(woff, roff, osize)
				tick = time.Now().Add(time.Second * 5)
			}
		}
	}
	close(rq)
	// free returned
	for i := 0; i < lenring; i++ {
		bio := ring[i]
		if bio != nil {
			buffs.free(bio.buf)
		}
	}
	// wait for the workers to exit; free in-flight
	for i := 0; i < npar; i++ {
		bio := <-cq
		if bio != nil && bio.buf != nil {
			buffs.free(bio.buf)
		}
	}
	if errstr != "" {
		return
	}
	size = woff
	if size != osize {
		glog.Errorf("Unexpected: osize %d != size %d", osize, size)
	}
	hashInBytes := md5.Sum(nil)[:16]
	md5hash = hex.EncodeToString(hashInBytes)
	return
}

func (gcpimpl *gcpimpl) readRange(gcpctx context.Context, objhdl *storage.ObjectHandle,
	rq, cq chan *bufIO, eq chan error, osize int64) {
	for {
		select {
		case bio := <-rq:
			if bio == nil {
				return
			}
			toread := int64(len(bio.buf))
			if toread > osize-bio.offset {
				toread = osize - bio.offset
				assert(toread > 0)
			}
			gcpreader, err := objhdl.NewRangeReader(gcpctx, bio.offset, toread)
			if err != nil {
				eq <- err
				cq <- bio
				return
			}
			read, err := gcpreader.Read(bio.buf[:toread])
			if err != nil {
				cq <- bio
				eq <- err
				return
			}
			if int64(read) != toread {
				glog.Infof("Warning: read != toread %d, %d", read, toread)
			}
			bio.read = int(toread) // FIXME
			cq <- bio
			gcpreader.Close()
		}
	}
}
