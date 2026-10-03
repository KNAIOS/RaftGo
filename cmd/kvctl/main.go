// kvctl retries transient failures with an unchanged client ID and sequence.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	nodes := flag.String("nodes", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083", "comma-separated API URLs")
	clientID := flag.String("client", "", "stable client ID (required for writes)")
	sequence := flag.Uint64("seq", 0, "monotonic sequence (required for writes)")
	value := flag.String("value", "", "new value")
	expected := flag.String("expected", "", "CAS expected value")
	exists := flag.Bool("exists", true, "CAS expects the key to exist")
	token := flag.String("token", os.Getenv("API_TOKEN"), "optional API bearer token")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl [flags] get|put|delete|cas|status|snapshot [key]")
		os.Exit(2)
	}
	op := args[0]
	method, path := "GET", "/v1/status"
	var body []byte
	if op != "status" && op != "snapshot" {
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "key required")
			os.Exit(2)
		}
		path = "/v1/kv/" + url.PathEscape(args[1])
	}
	switch op {
	case "get", "status":
	case "snapshot":
		method = "POST"
		path = "/v1/admin/snapshot"
	case "put", "delete", "cas":
		if *clientID == "" || *sequence == 0 {
			fmt.Fprintln(os.Stderr, "writes require -client and -seq")
			os.Exit(2)
		}
		method = map[string]string{"put": "PUT", "delete": "DELETE", "cas": "POST"}[op]
		if op == "cas" {
			path += "/cas"
		}
		body, _ = json.Marshal(map[string]interface{}{"client_id": *clientID, "sequence": *sequence, "value": *value, "expected": *expected, "expected_exists": *exists})
	default:
		fmt.Fprintln(os.Stderr, "unknown operation")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 8 * time.Second}
	endpoints := strings.Split(*nodes, ",")
	deadline := time.Now().Add(25 * time.Second)
	last := "no reachable node"
	for time.Now().Before(deadline) {
		for _, endpoint := range endpoints {
			req, err := http.NewRequest(method, strings.TrimRight(endpoint, "/")+path, bytes.NewReader(body))
			if err != nil {
				last = err.Error()
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if *token != "" {
				req.Header.Set("Authorization", "Bearer "+*token)
			}
			response, err := client.Do(req)
			if err != nil {
				last = err.Error()
				continue
			}
			data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
			response.Body.Close()
			if err != nil {
				last = err.Error()
				continue
			}
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				fmt.Println(string(data))
				return
			}
			last = string(data)
			var problem struct {
				Error string `json:"error"`
			}
			_ = json.Unmarshal(data, &problem)
			if response.StatusCode != 503 && !(response.StatusCode == 409 && problem.Error == "raft_unavailable") && !(response.StatusCode == 429 && problem.Error == "too_many_pending_requests") {
				fmt.Fprintln(os.Stderr, last)
				os.Exit(1)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "operation failed (write outcome may be unknown; retry with unchanged -client and -seq):", last)
	os.Exit(1)
}
