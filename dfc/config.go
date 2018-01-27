// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"time"

	"github.com/golang/glog"
)

// dfconfig specifies common daemon's configuration structure in JSON format.
type dfconfig struct {
	ID            string            `json:"id"`
	Logdir        string            `json:"logdir"`
	Loglevel      string            `json:"loglevel"`
	CloudProvider string            `json:"cloudprovider"`
	CloudBuckets  string            `json:"cloud_buckets"`
	LocalBuckets  string            `json:"local_buckets"`
	LBConf        string            `json:"lb_conf"`
	StatsTime     time.Duration     `json:"stats_time"`
	HttpTimeout   time.Duration     `json:"http_timeout"`
	Listen        listenconfig      `json:"listen"`
	Proxy         proxyconfig       `json:"proxy"`
	S3            s3config          `json:"s3"`
	Cacheconfig   cacheconfig       `json:"cacheconfig"`
	FSpaths       map[string]string `json:"fspaths"`
	TestFSP       testfspathconf    `json:"test_fspaths"`
	LegacyMode    bool              `json:"legacymode"`
}

const (
	amazoncloud = "aws"
	googlecloud = "gcp"
)

// s3config specifies  Amazon S3 specific configuration parameters
type s3config struct {
	Maxconcurrdownld uint32 `json:"maxconcurrdownld"` // Concurent Download for a session.
	Maxconcurrupld   uint32 `json:"maxconcurrupld"`   // Concurrent Upload for a session.
	Maxpartsize      uint64 `json:"maxpartsize"`      // Maximum part size for Upload and Download used for buffering.
}

// caching configuration
type cacheconfig struct {
	LowWM         uint32        `json:"lowwm"`           // capacity usage low watermark
	HighWM        uint32        `json:"highwm"`          // capacity usage high watermark
	DontEvictTime time.Duration `json:"dont_evict_time"` // eviction is not permitted during [atime, atime + dont]
}

type testfspathconf struct {
	Root     string `json:"root"`
	Count    int    `json:"count"`
	Instance int    `json:"instance"`
}

// daemon listenig params
type listenconfig struct {
	Proto string `json:"proto"` // Prototype : tcp, udp
	Port  string `json:"port"`  // Listening port.
}

// proxyconfig specifies proxy's well-known address as http://<ipaddress>:<portnumber>
type proxyconfig struct {
	URL      string `json:"url"`      // used to register caching servers
	Passthru bool   `json:"passthru"` // false: get then redirect, true (default): redirect right away
}

// Load and validate daemon's config
func initconfigparam(configfile, loglevel, role string, statstime time.Duration) error {
	getConfig(configfile)

	err := flag.Lookup("log_dir").Value.Set(ctx.config.Logdir)
	if err != nil {
		glog.Errorf("Failed to flag-set glog dir %q, err: %v", ctx.config.Logdir, err)
	}
	if err = CreateDir(ctx.config.Logdir); err != nil {
		glog.Errorf("Failed to create log dir %q, err: %v", ctx.config.Logdir, err)
		return err
	}
	if glog.V(3) {
		glog.Infof("Logdir %q Proto %s Port %s ID %s loglevel %s",
			ctx.config.Logdir, ctx.config.Listen.Proto,
			ctx.config.Listen.Port, ctx.config.ID, ctx.config.Loglevel)
	}
	// CLI override
	if statstime != 0 {
		ctx.config.StatsTime = statstime
	}
	if loglevel != "" {
		err = flag.Lookup("v").Value.Set(loglevel)
		ctx.config.Loglevel = loglevel
	} else {
		err = flag.Lookup("v").Value.Set(ctx.config.Loglevel)
	}
	if err != nil {
		//  Not fatal as it will use default logging level
		glog.Errorf("Failed to set loglevel %v", err)
	}
	glog.Infof("Verbosity: %s Config: %q Role: %s StatsTime %v",
		ctx.config.Loglevel, configfile, role, ctx.config.StatsTime)
	return err
}

// Read JSON config file and unmarshal json content into config struct.
func getConfig(fpath string) {
	raw, err := ioutil.ReadFile(fpath)
	if err != nil {
		glog.Errorf("Failed to read config %q, err: %v", fpath, err)
		os.Exit(1)
	}
	err = json.Unmarshal(raw, &ctx.config)
	if err != nil {
		glog.Errorf("Failed to json-unmarshal config %q, err: %v", fpath, err)
		os.Exit(1)
	}
}
