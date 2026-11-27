// Command loadgen is a dependency-free HTTP load generator used by
// hack/bench/run.sh to measure the e2e behaviour of the distributed rate limit
// plugin behind Traefik. Only the standard library is used, so it builds with
// the same toolchain as the plugin and needs no extra module.
//
// It drives a fixed number of workers against one or more URLs (round robin,
// to split the load between several Traefik instances), measures every request
// from the client side and prints a machine-readable summary:
//
//	requests=... status_200=... status_429=... errors=...
//	rps=...
//	lat_ms_min=... p50=... p90=... p95=... p99=... max=... mean=...
//	url0_status_200=... url1_status_200=...
//
// Latency is what a client observes (connection reuse, request, response body
// read), not what the server reports.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		urls     multiFlag
		headers  multiFlag
		workers  int
		duration time.Duration
		warmup   time.Duration
		timeout  time.Duration
	)

	flag.Var(&urls, "url", "target URL; repeat, or separate with commas, to split the load between several targets")
	flag.Var(&headers, "header", "extra request header in 'Name: value' form; repeatable")
	flag.IntVar(&workers, "workers", 32, "number of concurrent workers")
	flag.DurationVar(&duration, "duration", 10*time.Second, "measured duration")
	flag.DurationVar(&warmup, "warmup", 2*time.Second, "warmup duration, not measured")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "per-request timeout")
	flag.Parse()

	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "loadgen: at least one -url is required")
		os.Exit(2)
	}
	if workers < 1 {
		fmt.Fprintln(os.Stderr, "loadgen: -workers must be positive")
		os.Exit(2)
	}

	reqs := make([]*http.Request, 0, len(urls))
	for _, raw := range urls {
		req, err := http.NewRequest(http.MethodGet, raw, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "loadgen: bad URL %q: %v\n", raw, err)
			os.Exit(2)
		}
		for _, h := range headers {
			name, value, ok := strings.Cut(h, ":")
			if !ok {
				fmt.Fprintf(os.Stderr, "loadgen: bad header %q, want 'Name: value'\n", h)
				os.Exit(2)
			}
			req.Header.Set(strings.TrimSpace(name), strings.TrimSpace(value))
		}
		reqs = append(reqs, req)
	}

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        workers * len(reqs) * 2,
			MaxIdleConnsPerHost: workers * 2,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
		},
	}

	// Warm up the connections and the plugin (script load) without measuring.
	if warmup > 0 {
		run(client, reqs, workers, warmup, false)
	}

	stats := run(client, reqs, workers, duration, true)
	stats.print(duration)
}

// ---------------------------------------------------------------------------
// Measured run
// ---------------------------------------------------------------------------

type workerResult struct {
	latencies []time.Duration
	statuses  []int64 // indexed by the URL index, aggregated below
	failures  int64
}

type totals struct {
	requests  int64
	statuses  map[int]int64
	perURL    []map[int]int64
	latencies []time.Duration
	failures  int64
}

func run(client *http.Client, reqs []*http.Request, workers int, window time.Duration, measure bool) *totals {
	deadline := time.Now().Add(window)

	results := make([]workerResult, workers)
	statusSlots := int64(len(reqs)) * int64(countStatuses)

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()

			res := &results[w]
			if measure {
				res.latencies = make([]time.Duration, 0, 1024)
				res.statuses = make([]int64, statusSlots)
			}

			for i := 0; time.Now().Before(deadline); i++ {
				req := reqs[i%len(reqs)]

				start := time.Now()
				code, err := do(client, req)
				elapsed := time.Since(start)

				if !measure {
					continue
				}

				if err != nil {
					res.failures++
					continue
				}

				res.latencies = append(res.latencies, elapsed)
				slot := int64(i%len(reqs))*int64(countStatuses) + int64(statusIndex(code))
				atomic.AddInt64(&res.statuses[slot], 1)
			}
		}(w)
	}

	wg.Wait()

	if !measure {
		return &totals{}
	}

	out := &totals{
		statuses: map[int]int64{},
		perURL:   make([]map[int]int64, len(reqs)),
	}
	for i := range reqs {
		out.perURL[i] = map[int]int64{}
	}

	for w := range results {
		res := &results[w]

		out.latencies = append(out.latencies, res.latencies...)
		out.failures += res.failures

		for ul := range reqs {
			for si := 0; si < countStatuses; si++ {
				n := atomic.LoadInt64(&res.statuses[int64(ul)*int64(countStatuses)+int64(si)])
				if n == 0 {
					continue
				}
				code := statusCodes[si]
				out.statuses[code] += n
				out.perURL[ul][code] += n
				out.requests += n
			}
		}
	}

	sort.Slice(out.latencies, func(i, j int) bool { return out.latencies[i] < out.latencies[j] })

	return out
}

// The statuses worth telling apart: everything else lands in "other".
var statusCodes = []int{http.StatusOK, http.StatusTooManyRequests, 0}

var countStatuses = len(statusCodes)

func statusIndex(code int) int {
	for i, c := range statusCodes {
		if c == code {
			return i
		}
	}
	return countStatuses - 1
}

func do(client *http.Client, req *http.Request) (int, error) {
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}

	_, copyErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()

	if copyErr != nil {
		return resp.StatusCode, copyErr
	}

	return resp.StatusCode, closeErr
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

func (t *totals) print(window time.Duration) {
	percentile := func(p float64) float64 {
		if len(t.latencies) == 0 {
			return 0
		}

		idx := int(float64(len(t.latencies)-1) * p)
		if idx < 0 {
			idx = 0
		}

		return float64(t.latencies[idx].Microseconds()) / 1000
	}

	rps := float64(t.requests) / window.Seconds()

	fmt.Printf("requests=%d status_200=%d status_429=%d status_other=%d errors=%d\n",
		t.requests, t.statuses[200], t.statuses[429], t.statuses[0], t.failures)
	fmt.Printf("rps=%.1f\n", rps)

	if len(t.latencies) > 0 {
		var sum time.Duration
		for _, l := range t.latencies {
			sum += l
		}

		fmt.Printf("lat_ms_min=%.3f p50=%.3f p90=%.3f p95=%.3f p99=%.3f max=%.3f mean=%.3f\n",
			float64(t.latencies[0].Microseconds())/1000,
			percentile(0.50), percentile(0.90), percentile(0.95), percentile(0.99),
			float64(t.latencies[len(t.latencies)-1].Microseconds())/1000,
			float64(sum.Microseconds())/1000/float64(len(t.latencies)))
	}

	for i, per := range t.perURL {
		fmt.Printf("url%d_status_200=%d url%d_status_429=%d\n", i, per[200], i, per[429])
	}
}

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

// multiFlag collects repeated values, and also splits a comma separated value.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*m = append(*m, part)
		}
	}

	return nil
}
