// Package stats is used for keeping track of IO stats including number of ops,
// latency, throughput, etc. Assume single threaded access, it doesn't provide any locking on updates.
package stats

import (
	"math"
	"time"
)

type (
	stats struct {
		cnt     int64         // total # of requests
		bytes   int64         // total bytes by all requests
		errs    int64         // number of failed requests
		latency time.Duration // Accumulated request latency

		// self maintained fields
		minLatency time.Duration
		maxLatency time.Duration
	}

	// Stats records accumulated puts/gets information.
	Stats struct {
		Start     time.Time // time current stats started
		Put       stats
		Get       stats
		GetConfig stats
	}
)

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a >= b {
		return a
	}

	return b
}

// NewStats returns a new stats object with given time as the starting point
func NewStats(t time.Time) Stats {
	return Stats{
		Start: t,
		Put:   stats{minLatency: time.Duration(math.MaxInt64)},
		Get:   stats{minLatency: time.Duration(math.MaxInt64)},
	}
}

// NewStatsNow returns a new stats object using current time as the starting point
func NewStatsNow() Stats {
	return NewStats(time.Now())
}

// Add adds a request's result to the stats
func (s *stats) Add(size int64, delta time.Duration) {
	s.cnt++
	s.bytes += size
	s.latency += delta
	s.minLatency = minDuration(s.minLatency, delta)
	s.maxLatency = maxDuration(s.maxLatency, delta)
}

// AddErr increases the number of failed count by 1
func (s *stats) AddErr() {
	s.errs++
}

// Total returns the total number of requests.
func (s *stats) Total() int64 {
	return s.cnt
}

// TotalBytes returns the total number of bytes by all requests.
func (s *stats) TotalBytes() int64 {
	return s.bytes
}

// MinLatency returns the minimal latency in nano second.
func (s *stats) MinLatency() int64 {
	if s.cnt == 0 {
		return 0
	}
	return int64(s.minLatency)
}

// MaxLatency returns the maximal latency in nano second.
func (s *stats) MaxLatency() int64 {
	if s.cnt == 0 {
		return 0
	}
	return int64(s.maxLatency)
}

// AvgLatency returns the avg latency in nano second.
func (s *stats) AvgLatency() int64 {
	if s.cnt == 0 {
		return 0
	}
	return int64(s.latency) / s.cnt
}

// Throughput returns throughput of requests (bytes/per second).
func (s *stats) Throughput(start, end time.Time) int64 {
	if start == end {
		return 0
	}
	return int64(float64(s.bytes) / end.Sub(start).Seconds())
}

// TotalErrs returns the total number of failed requests.
func (s *stats) TotalErrs() int64 {
	return s.errs
}

// Aggregate adds another stats to self
func (s *stats) Aggregate(other stats) {
	s.cnt += other.cnt
	s.bytes += other.bytes
	s.errs += other.errs
	s.latency += other.latency

	s.minLatency = minDuration(s.minLatency, other.minLatency)
	s.maxLatency = maxDuration(s.maxLatency, other.maxLatency)
}

// Aggregate adds another stats to self
func (s *Stats) Aggregate(other Stats) {
	s.Get.Aggregate(other.Get)
	s.Put.Aggregate(other.Put)
	s.GetConfig.Aggregate(other.GetConfig)
}
