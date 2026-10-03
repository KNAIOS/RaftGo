// kvbench runs fixed-size mixed read/write workloads and validates per-client reads.
package main

import (
	"bytes"
	"encoding/json"
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

type sample struct {
	Latency float64
	Failed  bool
}

func main() {
	nodes := flag.String("nodes", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083", "API URLs")
	total := flag.Int("requests", 1000, "total measured requests")
	workers := flag.Int("workers", 8, "concurrent clients")
	size := flag.Int("value-size", 128, "value bytes")
	writePercent := flag.Int("writes", 20, "write percentage")
	flag.Parse()
	if *total < 1 || *workers < 1 || *workers > 1000 || *size < 1 || *size > 65536 || *writePercent < 0 || *writePercent > 100 {
		fmt.Fprintln(os.Stderr, "invalid benchmark configuration")
		os.Exit(2)
	}
	endpoints := strings.Split(*nodes, ",")
	token := os.Getenv("API_TOKEN")
	client := &http.Client{Timeout: 8 * time.Second}
	var preferred atomic.Int64
	call := func(method, path string, body []byte) (map[string]interface{}, error) {
		start := int(preferred.Load())
		for offset := 0; offset < len(endpoints); offset++ {
			index := (start + offset) % len(endpoints)
			req, err := http.NewRequest(method, strings.TrimRight(endpoints[index], "/")+path, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
			resp.Body.Close()
			if err != nil {
				continue
			}
			if resp.StatusCode != 200 {
				continue
			}
			var result map[string]interface{}
			if err = json.Unmarshal(data, &result); err != nil {
				return nil, err
			}
			preferred.Store(int64(index))
			return result, nil
		}
		return nil, fmt.Errorf("no successful response")
	}
	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	value := strings.Repeat("x", *size)
	// Initialize each worker's key before measurements.
	for i := 0; i < *workers; i++ {
		key := fmt.Sprintf("%s-%d", runID, i)
		body, _ := json.Marshal(map[string]interface{}{"client_id": key, "sequence": 1, "value": value})
		if _, err := call("PUT", "/v1/kv/"+key, body); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	var cursor atomic.Int64
	results := make(chan sample, *total)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", runID, i)
			sequence := uint64(1)
			for {
				index := int(cursor.Add(1)) - 1
				if index >= *total {
					return
				}
				method := "GET"
				var body []byte
				// Multiplication spreads writes across workers while preserving the configured ratio.
				if (index*37)%100 < *writePercent {
					method = "PUT"
					sequence++
					body, _ = json.Marshal(map[string]interface{}{"client_id": key, "sequence": sequence, "value": value})
				}
				before := time.Now()
				result, err := call(method, "/v1/kv/"+key, body)
				failed := err != nil
				if !failed {
					failed = result["value"] != value || result["exists"] != true
				}
				results <- sample{Latency: float64(time.Since(before).Microseconds()) / 1000, Failed: failed}
			}
		}(i)
	}
	wg.Wait()
	seconds := time.Since(start).Seconds()
	close(results)
	latencies := make([]float64, 0, *total)
	errors := 0
	for s := range results {
		if s.Failed {
			errors++
		}
		latencies = append(latencies, s.Latency)
	}
	sort.Float64s(latencies)
	percentile := func(p float64) float64 { index := int(float64(len(latencies)-1) * p); return latencies[index] }
	// Benchmark keys and session records remain for reproducibility.
	// The benchmark is bounded by the documented 10,000-session capacity.
	report := map[string]interface{}{"requests": *total, "workers": *workers, "value_bytes": *size, "write_percent": *writePercent, "errors": errors, "elapsed_seconds": seconds, "requests_per_second": float64(*total) / seconds, "successful_requests_per_second": float64(*total-errors) / seconds, "latency_ms_p50": percentile(.50), "latency_ms_p95": percentile(.95), "latency_ms_p99": percentile(.99), "run_id": runID, "note": "latencies include retries; single-host Docker demonstration, not a production benchmark"}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(encoded))
	if errors > 0 {
		os.Exit(1)
	}
}
