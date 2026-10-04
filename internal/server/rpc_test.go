package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kvv1 "go_prj/api/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRPCAndHTTPShareState(t *testing.T) {
	cfg := Config{ID: "test", DataDir: t.TempDir(), RaftAddr: "127.0.0.1:0", Advertise: "127.0.0.1:0", GRPCAddr: "127.0.0.1:0", Token: "secret", Bootstrap: true, Peers: []Peer{{ID: "test", Raft: "127.0.0.1:0", API: "http://test"}}}
	node, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	deadline := time.Now().Add(8 * time.Second)
	for node.raft.VerifyLeader().Error() != nil {
		if time.Now().After(deadline) {
			t.Fatal("leader not ready")
		}
		time.Sleep(30 * time.Millisecond)
	}
	conn, err := grpc.NewClient(node.grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kvv1.NewKVServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err = client.Get(ctx, &kvv1.GetRequest{Key: "k"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated RPC accepted: %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer secret")
	if _, err = client.Put(ctx, &kvv1.MutationRequest{Key: "k", Value: "a", ClientId: "c", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(node.Router())
	defer api.Close()
	request := &kvv1.MutationRequest{Key: "k", Value: "b", ClientId: "c", Sequence: 2, Expected: "a", ExpectedExists: true}
	body := []byte(`{"value":"b","client_id":"c","sequence":2,"expected":"a","expected_exists":true}`)
	req, _ := http.NewRequest("POST", api.URL+"/v1/kv/k/cas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP CAS: %s", data)
	}
	var result struct {
		Applied bool `json:"applied"`
	}
	_ = json.Unmarshal(data, &result)
	if !result.Applied {
		t.Fatal("CAS did not apply")
	}
	duplicate, err := client.CompareAndSwap(ctx, request)
	if err != nil || !duplicate.Applied || duplicate.Value != "b" {
		t.Fatalf("cross-transport duplicate lost original result: %+v %v", duplicate, err)
	}
	request.Value = "conflict"
	if _, err = client.CompareAndSwap(ctx, request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict status: %v", err)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: "k"})
	if err != nil || got.Value != "b" {
		t.Fatalf("read: %+v %v", got, err)
	}
	if _, err = client.Put(ctx, &kvv1.MutationRequest{Key: "k", Value: strings.Repeat("x", (64<<10)+1), ClientId: "c", Sequence: 3}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RPC value limit bypass: %v", err)
	}
	if atomic.LoadUint64(&node.rpcCalls) < 6 {
		t.Fatal("gateway did not traverse RPC interceptor")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err = client.Get(canceled, &kvv1.GetRequest{Key: "k"}); status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err = client.Delete(ctx, &kvv1.MutationRequest{Key: "k", ClientId: "c", Sequence: 3}); err != nil {
		t.Fatal(err)
	}
	got, err = client.Get(ctx, &kvv1.GetRequest{Key: "k"})
	if err != nil || got.Exists {
		t.Fatalf("delete: %+v %v", got, err)
	}
}
