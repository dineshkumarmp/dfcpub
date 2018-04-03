package stats_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmd/dfcloader/stats"
)

func verify(t *testing.T, msg string, exp, act int64) {
	if exp != act {
		t.Fatalf("Error: %s, expected = %d, actual = %d", msg, exp, act)
	}
}

func TestStats(t *testing.T) {
	start := time.Now()
	s := stats.NewStats(start)

	// basic put
	s.Put.Add(100, time.Duration(100*time.Millisecond))
	s.Put.Add(200, time.Duration(20*time.Millisecond))
	s.Put.AddErr()
	s.Put.Add(50, time.Duration(30*time.Millisecond))

	verify(t, "Total puts", 3, s.Put.Total())
	verify(t, "Total put bytes", 350, s.Put.TotalBytes())
	verify(t, "Min put latency", 20000000, s.Put.MinLatency())
	verify(t, "Avg put latency", 50000000, int64(s.Put.AvgLatency()))
	verify(t, "Max put latency", 100000000, s.Put.MaxLatency())
	verify(t, "Put throughput", 5, s.Put.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Failed puts", 1, s.Put.TotalErrs())

	// basic get
	s.Get.Add(100, time.Duration(100*time.Millisecond))
	s.Get.Add(200, time.Duration(20*time.Millisecond))
	s.Get.AddErr()
	s.Get.Add(50, time.Duration(10*time.Millisecond))
	s.Get.AddErr()
	s.Get.Add(200, time.Duration(190*time.Millisecond))

	verify(t, "Total gets", 4, s.Get.Total())
	verify(t, "Total get bytes", 550, s.Get.TotalBytes())
	verify(t, "Min get latency", 10000000, int64(s.Get.MinLatency()))
	verify(t, "Avg get latency", 80000000, int64(s.Get.AvgLatency()))
	verify(t, "Max get latency", 190000000, int64(s.Get.MaxLatency()))
	verify(t, "Get throughput", 5, s.Get.Throughput(start, start.Add(110*time.Second)))
	verify(t, "Failed gets", 2, s.Get.TotalErrs())

	// accumulate non empty stats on top of empty stats
	total := stats.NewStats(start)
	total.Aggregate(s)
	verify(t, "Total puts", 3, total.Put.Total())
	verify(t, "Total put bytes", 350, total.Put.TotalBytes())
	verify(t, "Min put latency", 20000000, total.Put.MinLatency())
	verify(t, "Avg put latency", 50000000, int64(total.Put.AvgLatency()))
	verify(t, "Max put latency", 100000000, total.Put.MaxLatency())
	verify(t, "Put throughput", 5, total.Put.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Failed puts", 1, total.Put.TotalErrs())

	// accumulate empty stats on top of non empty stats
	s = stats.NewStats(start)
	total.Aggregate(s)
	verify(t, "Min get latency", 10000000, int64(total.Get.MinLatency()))
	verify(t, "Avg get latency", 80000000, int64(total.Get.AvgLatency()))
	verify(t, "Max get latency", 190000000, int64(total.Get.MaxLatency()))

	// accumulate non empty stats on top of non empty stats
	s.Get.AddErr()
	s.Put.AddErr()
	total.Aggregate(s)
	verify(t, "Failed puts", 2, total.Put.TotalErrs())
	verify(t, "Failed gets", 3, total.Get.TotalErrs())

	s.Get.Add(1000, time.Duration(5*time.Millisecond))
	s.Put.Add(1, time.Duration(1000*time.Millisecond))
	total.Aggregate(s)
	verify(t, "Total puts", 4, total.Put.Total())
	verify(t, "Total put bytes", 351, total.Put.TotalBytes())
	verify(t, "Min put latency", 20000000, total.Put.MinLatency())
	verify(t, "Avg put latency", 287500000, int64(total.Put.AvgLatency()))
	verify(t, "Max put latency", 1000000000, total.Put.MaxLatency())
	verify(t, "Put throughput", 5, total.Put.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Total gets", 5, total.Get.Total())
	verify(t, "Total get bytes", 1550, total.Get.TotalBytes())
	verify(t, "Min get latency", 5000000, int64(total.Get.MinLatency()))
	verify(t, "Avg get latency", 65000000, int64(total.Get.AvgLatency()))
	verify(t, "Max get latency", 190000000, int64(total.Get.MaxLatency()))
	verify(t, "Get throughput", 14, total.Get.Throughput(start, start.Add(110*time.Second)))
}
