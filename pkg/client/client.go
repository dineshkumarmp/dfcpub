// Package client provides common operations for files in cloud storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/OneOfOne/xxhash"
)

var (
	transport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 600 * time.Second,
	}

	client = &http.Client{
		Timeout:   600 * time.Second,
		Transport: transport,
	}

	ProxyProto      = "http"
	ProxyIP         = "localhost"
	ProxyPort       = 8080
	RestAPIVersion  = "v1"
	RestAPIResource = "files"
)

type ReqError struct {
	code    int
	message string
}

type BucketProps struct {
	CloudProvider string
	Versioning    string
}

// Reader is the interface a client works with to read in data and send to a HTTP server
type Reader interface {
	io.ReadCloser
	io.Seeker
	Open() (io.ReadCloser, error)
	XXHash() string
	Description() string
}

type bytesReaderCloser struct {
	bytes.Reader
}

func (q *bytesReaderCloser) Close() error {
	return nil
}

func (err ReqError) Error() string {
	return err.message
}

func newReqError(msg string, code int) ReqError {
	return ReqError{
		code:    code,
		message: msg,
	}
}

func Tcping(url string) (err error) {
	addr := strings.TrimPrefix(url, "http://")
	if addr == url {
		addr = strings.TrimPrefix(url, "https://")
	}
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return
}

func discardResponse(r *http.Response, err error, src string) (int64, error) {
	var len int64

	if err == nil {
		if r.StatusCode >= http.StatusBadRequest {
			return 0, fmt.Errorf("Bad status code from %s: http status %d", src, r.StatusCode)
		}
		bufreader := bufio.NewReader(r.Body)
		if len, err = dfc.ReadToNull(bufreader); err != nil {
			return 0, fmt.Errorf("Failed to read http response, err: %v", err)
		}
	} else {
		return 0, fmt.Errorf("%s failed, err: %v", src, err)
	}

	return len, nil
}

func emitError(r *http.Response, err error, errch chan error) {
	if err == nil || errch == nil {
		return
	}

	if r != nil {
		errObj := newReqError(err.Error(), r.StatusCode)
		errch <- errObj
	} else {
		errch <- err
	}
}

func Get(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool, validate bool) (int64, error) {
	var (
		hash, hdhash, hdhashtype string
		errstr                   string
	)
	if wg != nil {
		defer wg.Done()
	}
	url := proxyurl + "/v1/files/" + bucket + "/" + keyname
	r, err := http.Get(url)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if validate && err == nil {
		hdhash = r.Header.Get(dfc.HeaderDfcChecksumVal)
		hdhashtype = r.Header.Get(dfc.HeaderDfcChecksumType)
		if hdhashtype == dfc.ChecksumXXHash {
			xx := xxhash.New64()
			if hash, errstr = dfc.ComputeXXHash(r.Body, nil, xx); errstr != "" {
				if errch != nil {
					errch <- errors.New(errstr)
				}
			}
			if hdhash != hash {
				s := fmt.Sprintf("Header's hash %s doesn't match the file's %s \n", hdhash, hash)
				if errch != nil {
					errch <- errors.New(s)
				}
			} else {
				if !silent {
					fmt.Printf("Header's hash %s matches the file's %s \n", hdhash, hash)
				}
			}
		}
	}
	len, err := discardResponse(r, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket))
	emitError(r, err, errch)
	return len, err
}

func Del(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}

	delurl := proxyurl + "/v1/files/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("DEL: %s\n", keyname)
	}
	req, httperr := http.NewRequest(http.MethodDelete, delurl, nil)
	if httperr != nil {
		err = fmt.Errorf("Failed to create new http request, err: %v", httperr)
		emitError(nil, err, errch)
		return err
	}

	r, httperr := client.Do(req)
	if httperr != nil {
		err = fmt.Errorf("Failed to delete file, err: %v", httperr)
		emitError(nil, err, errch)
		return err
	}

	defer func() {
		r.Body.Close()
	}()

	_, err = discardResponse(r, err, "DELETE")
	emitError(r, err, errch)
	return err
}

func ListBucket(proxyurl, bucket string, injson []byte) (*dfc.BucketList, error) {
	var (
		url     = proxyurl + "/v1/files/" + bucket
		err     error
		request *http.Request
		r       *http.Response
	)

	if len(injson) == 0 {
		r, err = client.Get(url)
	} else {
		request, err = http.NewRequest("GET", url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			r, err = client.Do(request)
		}
	}
	if err != nil {
		return nil, err
	}
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("List bucket %s failed, HTTP status %d", bucket, r.StatusCode)
	}

	defer func() {
		r.Body.Close()
	}()
	var reslist = &dfc.BucketList{}
	reslist.Entries = make([]*dfc.BucketEntry, 0, 1000)
	b, err := ioutil.ReadAll(r.Body)

	if err == nil {
		err = json.Unmarshal(b, reslist)
		if err != nil {
			return nil, fmt.Errorf("Failed to json-unmarshal, err: %v [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("Failed to read json, err: %v", err)
	}

	return reslist, nil
}

func Evict(proxyurl, bucket string, fname string) error {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	EvictMsg := dfc.ActionMsg{Action: dfc.ActEvict}
	EvictMsg.Name = bucket + "/" + fname
	injson, err = json.Marshal(EvictMsg)
	if err != nil {
		return fmt.Errorf("Failed to marshal EvictMsg: %v", err)
	}

	req, err = http.NewRequest("DELETE", proxyurl+"/v1/files/"+bucket+"/"+fname, bytes.NewBuffer(injson))
	if err != nil {
		return fmt.Errorf("Failed to create request: %v", err)
	}

	r, err = client.Do(req)
	if r != nil {
		r.Body.Close()
	}

	if err != nil {
		return err
	}

	return nil
}

func doListRangeCall(proxyurl, bucket, action, method string, listrangemsg interface{}, wait bool) error {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	actionMsg := dfc.ActionMsg{Action: action, Value: listrangemsg}
	injson, err = json.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("Failed to marhsal ActionMsg: %v", err)
	}
	req, err = http.NewRequest(method, proxyurl+"/v1/files/"+bucket+"/", bytes.NewBuffer(injson))
	if err != nil {
		return fmt.Errorf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if wait {
		r, err = client.Do(req)
	} else {
		r, err = client.Do(req)
	}
	if r != nil {
		r.Body.Close()
	}
	return err
}

func PrefetchList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func PrefetchRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: prefetchMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActPrefetch, http.MethodPost, prefetchMsg, wait)
}

func DeleteList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func DeleteRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActDelete, http.MethodDelete, deleteMsg, wait)
}

func EvictList(proxyurl, bucket string, fileslist []string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := dfc.ListMsg{Objnames: fileslist, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActEvict, http.MethodDelete, evictMsg, wait)
}

func EvictRange(proxyurl, bucket, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	rangeListMsgBase := dfc.RangeListMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := dfc.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, RangeListMsgBase: rangeListMsgBase}
	return doListRangeCall(proxyurl, bucket, dfc.ActEvict, http.MethodDelete, evictMsg, wait)
}

// fastRandomFilename is taken from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func FastRandomFilename(src *rand.Rand, fnlen int) string {
	b := make([]byte, fnlen)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := fnlen-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func HeadBucket(proxyurl, bucket string) (bucketprops *BucketProps, err error) {
	var (
		url = proxyurl + "/v1/files/" + bucket
		r   *http.Response
	)
	bucketprops = &BucketProps{}
	r, err = client.Head(url)
	if err != nil {
		return
	}
	defer func() {
		r.Body.Close()
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("Head bucket %s failed, HTTP status %d", bucket, r.StatusCode)
		return
	}
	bucketprops.CloudProvider = r.Header.Get(dfc.CloudProvider)
	bucketprops.Versioning = r.Header.Get(dfc.Versioning)
	return
}

func checkHTTPStatus(resp *http.Response, op string) error {
	if resp.StatusCode >= http.StatusBadRequest {
		return ReqError{
			code:    resp.StatusCode,
			message: fmt.Sprintf("Bad status code from %s", op),
		}
	}

	return nil
}

func discardHTTPResp(resp *http.Response) {
	bufreader := bufio.NewReader(resp.Body)
	dfc.ReadToNull(bufreader)
}

// Put sends a PUT request to the given URL
func Put(proxyURL string, reader Reader, bucket string, key string, silent bool) error {
	url := proxyURL + "/v1/files/" + bucket + "/" + key

	if !silent {
		fmt.Printf("PUT: %s/%s\n", bucket, key)
	}

	handle, err := reader.Open()
	if err != nil {
		return fmt.Errorf("Failed to open reader, err: %v", err)
	}
	defer handle.Close()

	req, err := http.NewRequest(http.MethodPut, url, handle)
	if err != nil {
		return fmt.Errorf("Failed to create new http request, err: %v", err)
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return reader.Open()
	}

	if reader.XXHash() != "" {
		req.Header.Set(dfc.HeaderDfcChecksumType, dfc.ChecksumXXHash)
		req.Header.Set(dfc.HeaderDfcChecksumVal, reader.XXHash())
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	err = checkHTTPStatus(resp, "PUT")
	discardHTTPResp(resp)
	return err
}

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL string, reader Reader, bucket string, key string, errch chan error, silent bool) {
	defer wg.Done()
	err := Put(proxyURL, reader, bucket, key, silent)
	if err != nil {
		if errch == nil {
			fmt.Println("Error channel is not given, do know how to report error", err)
		} else {
			errch <- err
		}
	}
}

// CreateLocalBucket sends a HTTP request to a proxy and asks it to create a local bucket
func CreateLocalBucket(proxyURL, bucket string) error {
	msg, err := json.Marshal(dfc.ActionMsg{Action: dfc.ActCreateLB})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", proxyURL+"/v1/files/"+bucket, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	r, err := client.Do(req)
	if r != nil {
		r.Body.Close()
	}

	// FIXME: A few places are doing this already, need to address them
	time.Sleep(time.Second * 2)
	return err
}

// DestroyLocalBucket deletes a local bucket
func DestroyLocalBucket(proxyURL, bucket string) error {
	msg, err := json.Marshal(dfc.ActionMsg{Action: dfc.ActDestroyLB})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", proxyURL+"/v1/files/"+bucket, bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}

	return err
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL, bucket, prefix string) ([]string, error) {
	msg, err := json.Marshal(&dfc.GetMsg{GetPrefix: prefix})
	if err != nil {
		return nil, err
	}

	data, err := ListBucket(proxyURL, bucket, msg)
	if err != nil {
		return nil, err
	}

	var objs []string
	for _, obj := range data.Entries {
		// Skip directories
		if obj.Name[len(obj.Name)-1] != '/' {
			objs = append(objs, obj.Name)
		}
	}

	return objs, nil
}
