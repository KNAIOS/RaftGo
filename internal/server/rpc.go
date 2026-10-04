package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	kvv1 "go_prj/api/kv/v1"
	"go_prj/internal/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type kvRPC struct {
	kvv1.UnimplementedKVServiceServer
	node *Node
}

func (n *Node) rpcInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	defer func() {
		if value := recover(); value != nil {
			log.Printf("RPC panic in %s: %v", info.FullMethod, value)
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	if n.cfg.Token != "" {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+n.cfg.Token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "unauthorized")
		}
	}
	atomic.AddUint64(&n.rpcCalls, 1)
	return handler(ctx, req)
}

func (n *Node) startRPC() error {
	addr := n.cfg.GRPCAddr
	if addr == "" {
		addr = "127.0.0.1:9090"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(n.rpcInterceptor), grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20), grpc.MaxConcurrentStreams(128))
	kvv1.RegisterKVServiceServer(server, &kvRPC{node: n})
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		return err
	}
	// The Gin gateway dials a real loopback TCP connection using generated stubs.
	conn, err := grpc.NewClient(net.JoinHostPort("127.0.0.1", port), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(1<<20)))
	if err != nil {
		listener.Close()
		return err
	}
	n.grpcServer = server
	n.grpcConn = conn
	n.rpcClient = kvv1.NewKVServiceClient(conn)
	n.grpcEndpoint = listener.Addr().String()
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()
	return nil
}

func (n *Node) rpcFailure(code codes.Code, reason string, err error) error {
	id, api := n.leader()
	problem := status.New(code, err.Error())
	detailed, detailErr := problem.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "raftgo.kv.v1", Metadata: map[string]string{"leader_id": id, "leader_api": api, "outcome": "unknown; retry writes with unchanged client_id and sequence"}})
	if detailErr != nil {
		return problem.Err()
	}
	return detailed.Err()
}

func (n *Node) applyRPC(ctx context.Context, cmd store.Command) (store.Result, error) {
	if err := ctx.Err(); err != nil {
		return store.Result{}, status.FromContextError(err).Err()
	}
	if n.raft.State() != raft.Leader {
		return store.Result{}, n.rpcFailure(codes.FailedPrecondition, "NOT_LEADER", raft.ErrNotLeader)
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return store.Result{}, status.Error(codes.InvalidArgument, "invalid command")
	}
	select {
	case n.pending <- struct{}{}:
	default:
		return store.Result{}, n.rpcFailure(codes.ResourceExhausted, "TOO_MANY_PENDING_REQUESTS", fmt.Errorf("too_many_pending_requests"))
	}
	future := n.raft.Apply(data, 5*time.Second)
	completed := make(chan error, 1)
	go func() { err := future.Error(); <-n.pending; completed <- err }()
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	select {
	case err = <-completed:
	case <-timer.C:
		return store.Result{}, n.rpcFailure(codes.DeadlineExceeded, "APPLICATION_TIMEOUT", fmt.Errorf("application deadline exceeded"))
	case <-ctx.Done():
		return store.Result{}, n.rpcFailure(status.FromContextError(ctx.Err()).Code(), "REQUEST_CANCELED", ctx.Err())
	}
	if err != nil {
		code, reason := codes.Unavailable, "RAFT_UNAVAILABLE"
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			code, reason = codes.FailedPrecondition, "NOT_LEADER"
		}
		return store.Result{}, n.rpcFailure(code, reason, err)
	}
	result, ok := future.Response().(store.Result)
	if !ok {
		return store.Result{}, status.Error(codes.Internal, "invalid FSM result")
	}
	if result.Error != "" {
		code := codes.FailedPrecondition
		if result.Error == "sequence_conflict" {
			code = codes.AlreadyExists
		}
		if result.Error == "session_limit" || result.Error == "store_full" {
			code = codes.ResourceExhausted
		}
		return store.Result{}, n.rpcFailure(code, result.Error, fmt.Errorf("%s", result.Error))
	}
	return result, nil
}

func toProto(result store.Result) *kvv1.Result {
	return &kvv1.Result{Value: result.Value, Exists: result.Exists, Applied: result.Applied, Error: result.Error}
}
func (s *kvRPC) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.Result, error) {
	if req == nil || req.Key == "" || len(req.Key) > 256 {
		return nil, status.Error(codes.InvalidArgument, "invalid key")
	}
	result, err := s.node.applyRPC(ctx, store.Command{Op: "get", Key: req.Key})
	if err != nil {
		return nil, err
	}
	return toProto(result), nil
}
func (s *kvRPC) mutate(ctx context.Context, op string, req *kvv1.MutationRequest) (*kvv1.Result, error) {
	if req == nil || req.Key == "" || len(req.Key) > 256 || len(req.Value) > 64<<10 || len(req.Expected) > 64<<10 || req.ClientId == "" || len(req.ClientId) > 128 || req.Sequence == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid_key_value_or_session")
	}
	cmd := store.Command{Op: op, Key: req.Key, Value: req.Value, ClientID: req.ClientId, Sequence: req.Sequence, Expected: req.Expected, ExpectedExists: req.ExpectedExists}
	result, err := s.node.applyRPC(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return toProto(result), nil
}
func (s *kvRPC) Put(ctx context.Context, req *kvv1.MutationRequest) (*kvv1.Result, error) {
	return s.mutate(ctx, "put", req)
}
func (s *kvRPC) Delete(ctx context.Context, req *kvv1.MutationRequest) (*kvv1.Result, error) {
	return s.mutate(ctx, "delete", req)
}
func (s *kvRPC) CompareAndSwap(ctx context.Context, req *kvv1.MutationRequest) (*kvv1.Result, error) {
	return s.mutate(ctx, "cas", req)
}

func (n *Node) gatewayContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, 7*time.Second)
	if n.cfg.Token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+n.cfg.Token)
	}
	return ctx, cancel
}
