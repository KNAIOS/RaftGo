// kvgrpc invokes the generated Protobuf client and preserves write identity on retry.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	kvv1 "go_prj/api/kv/v1"
	"go_prj/internal/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	case codes.FailedPrecondition, codes.ResourceExhausted:
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				return info.Reason == "NOT_LEADER" || info.Reason == "TOO_MANY_PENDING_REQUESTS"
			}
		}
	}
	return false
}

func main() {
	nodes := flag.String("nodes", "127.0.0.1:19091,127.0.0.1:19092,127.0.0.1:19093", "comma-separated gRPC endpoints")
	id := flag.String("client", "", "stable client ID for mutations")
	seq := flag.Uint64("seq", 0, "monotonic write sequence")
	value := flag.String("value", "", "new value")
	expected := flag.String("expected", "", "CAS expected value")
	exists := flag.Bool("exists", true, "CAS expects key to exist")
	token := flag.String("token", os.Getenv("API_TOKEN"), "optional bearer token")
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvgrpc [flags] get|put|delete|cas key")
		os.Exit(2)
	}
	op, key := args[0], args[1]
	if op != "get" && op != "put" && op != "delete" && op != "cas" {
		fmt.Fprintln(os.Stderr, "unknown operation")
		os.Exit(2)
	}
	if op != "get" && (*id == "" || *seq == 0) {
		fmt.Fprintln(os.Stderr, "writes require -client and -seq")
		os.Exit(2)
	}
	clients := []kvv1.KVServiceClient{}
	connections := []*grpc.ClientConn{}
	for _, endpoint := range strings.Split(*nodes, ",") {
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		connections = append(connections, conn)
		clients = append(clients, kvv1.NewKVServiceClient(conn))
	}
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if *token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
	}
	request := &kvv1.MutationRequest{Key: key, Value: *value, ClientId: *id, Sequence: *seq, Expected: *expected, ExpectedExists: *exists}
	var last error
	for ctx.Err() == nil {
		for _, client := range clients {
			callCtx, callCancel := context.WithTimeout(ctx, 8*time.Second)
			var response *kvv1.Result
			switch op {
			case "get":
				response, last = client.Get(callCtx, &kvv1.GetRequest{Key: key})
			case "put":
				response, last = client.Put(callCtx, request)
			case "delete":
				response, last = client.Delete(callCtx, request)
			case "cas":
				response, last = client.CompareAndSwap(callCtx, request)
			}
			callCancel()
			if last == nil {
				data, _ := json.Marshal(store.Result{Value: response.Value, Exists: response.Exists, Applied: response.Applied, Error: response.Error})
				fmt.Println(string(data))
				return
			}
			if !retryable(last) {
				fmt.Fprintln(os.Stderr, last)
				os.Exit(1)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	fmt.Fprintln(os.Stderr, "request failed; write outcome may be unknown; retry unchanged client and sequence:", last)
	os.Exit(1)
}
