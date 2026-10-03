package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go_prj/internal/store"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Peer struct {
	ID   string `json:"id"`
	Raft string `json:"raft"`
	API  string `json:"api"`
}
type Config struct {
	ID, HTTPAddr, RaftAddr, Advertise, DataDir, Token string
	Bootstrap                                         bool
	Peers                                             []Peer
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func FromEnv() (Config, error) {
	cfg := Config{ID: env("NODE_ID", "node1"), HTTPAddr: env("HTTP_ADDR", ":8080"), RaftAddr: env("RAFT_ADDR", ":7000"), Advertise: env("RAFT_ADVERTISE", "127.0.0.1:7000"), DataDir: env("DATA_DIR", "data/node1"), Token: os.Getenv("API_TOKEN")}
	var err error
	cfg.Bootstrap, err = strconv.ParseBool(env("BOOTSTRAP", "false"))
	if err != nil {
		return cfg, err
	}
	if err = json.Unmarshal([]byte(env("PEERS", `[{"id":"node1","raft":"127.0.0.1:7000","api":"http://127.0.0.1:8080"}]`)), &cfg.Peers); err != nil {
		return cfg, err
	}
	if len(cfg.Peers) == 0 {
		return cfg, fmt.Errorf("empty peers")
	}
	ids, addresses := map[string]bool{}, map[string]bool{}
	self := false
	for _, p := range cfg.Peers {
		if p.ID == "" || ids[p.ID] || addresses[p.Raft] {
			return cfg, fmt.Errorf("invalid or duplicate peer")
		}
		if _, _, err = net.SplitHostPort(p.Raft); err != nil {
			return cfg, err
		}
		ids[p.ID] = true
		addresses[p.Raft] = true
		if p.ID == cfg.ID {
			self = p.Raft == cfg.Advertise
		}
	}
	if !self {
		return cfg, fmt.Errorf("local node must match peers and advertised address")
	}
	return cfg, nil
}

type Node struct {
	cfg       Config
	raft      *raft.Raft
	transport *raft.NetworkTransport
	db        *raftboltdb.BoltStore
	pending   chan struct{}
}

func Open(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}
	db, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	fail := func(err error) (*Node, error) { _ = transport.Close(); _ = db.Close(); return nil, err }
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return fail(err)
	}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.SnapshotThreshold = 256
	rc.TrailingLogs = 128
	rc.SnapshotInterval = 30 * time.Second
	hasState, err := raft.HasExistingState(db, db, snapshots)
	if err != nil {
		return fail(err)
	}
	r, err := raft.NewRaft(rc, store.New(), db, db, snapshots, transport)
	if err != nil {
		return fail(err)
	}
	n := &Node{cfg: cfg, raft: r, transport: transport, db: db, pending: make(chan struct{}, 128)}
	if cfg.Bootstrap && !hasState {
		peers := raft.Configuration{}
		for _, p := range cfg.Peers {
			peers.Servers = append(peers.Servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft), Suffrage: raft.Voter})
		}
		if err = r.BootstrapCluster(peers).Error(); err != nil {
			_ = n.Close()
			return nil, err
		}
	}
	return n, nil
}
func (n *Node) Close() error {
	err := n.raft.Shutdown().Error()
	_ = n.transport.Close()
	dbErr := n.db.Close()
	if err != nil {
		return err
	}
	return dbErr
}
func (n *Node) leader() (string, string) {
	_, id := n.raft.LeaderWithID()
	for _, p := range n.cfg.Peers {
		if p.ID == string(id) {
			return string(id), p.API
		}
	}
	return string(id), ""
}
func (n *Node) raftError(c *gin.Context, err error) {
	id, api := n.leader()
	code := 503
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
		code = 409
	}
	c.JSON(code, gin.H{"error": "raft_unavailable", "detail": err.Error(), "leader_id": id, "leader_api": api, "outcome": "unknown; retry writes with the same client_id and sequence"})
}
func (n *Node) execute(c *gin.Context, cmd store.Command) {
	if n.raft.State() != raft.Leader {
		n.raftError(c, raft.ErrNotLeader)
		return
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid_request"})
		return
	}
	select {
	case n.pending <- struct{}{}:
	default:
		c.JSON(429, gin.H{"error": "too_many_pending_requests"})
		return
	}
	future := n.raft.Apply(data, 5*time.Second)
	completed := make(chan error, 1)
	go func() { err := future.Error(); <-n.pending; completed <- err }()
	select {
	case err = <-completed:
	case <-time.After(6 * time.Second):
		n.raftError(c, fmt.Errorf("application deadline exceeded"))
		return
	case <-c.Request.Context().Done():
		return
	}
	if err != nil {
		n.raftError(c, err)
		return
	}
	result, ok := future.Response().(store.Result)
	if !ok {
		c.JSON(500, gin.H{"error": "invalid_result"})
		return
	}
	code := 200
	if result.Error != "" {
		code = 409
		if result.Error == "session_limit" {
			code = 429
		}
	}
	c.JSON(code, result)
}
func (n *Node) Router() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "alive", "node_id": n.cfg.ID}) })
	r.Use(func(c *gin.Context) {
		if n.cfg.Token != "" && subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+n.cfg.Token)) != 1 {
			c.AbortWithStatusJSON(401, gin.H{"error": "unauthorized"})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
		c.Next()
	})
	r.GET("/readyz", func(c *gin.Context) {
		if err := n.raft.VerifyLeader().Error(); err != nil {
			n.raftError(c, err)
			return
		}
		c.JSON(200, gin.H{"status": "leader_ready"})
	})
	r.GET("/v1/status", func(c *gin.Context) {
		id, api := n.leader()
		c.JSON(200, gin.H{"node_id": n.cfg.ID, "state": n.raft.State().String(), "leader_id": id, "leader_api": api, "stats": n.raft.Stats()})
	})
	r.GET("/v1/kv/:key", func(c *gin.Context) {
		key := c.Param("key")
		if len(key) > 256 {
			c.JSON(400, gin.H{"error": "key_too_long"})
			return
		}
		n.execute(c, store.Command{Op: "get", Key: key})
	})
	write := func(op string) gin.HandlerFunc {
		return func(c *gin.Context) {
			var cmd store.Command
			if err := c.ShouldBindJSON(&cmd); err != nil {
				c.JSON(400, gin.H{"error": "invalid_json"})
				return
			}
			cmd.Op = op
			cmd.Key = c.Param("key")
			if len(cmd.Key) > 256 || len(cmd.Value) > 64<<10 || len(cmd.Expected) > 64<<10 || len(cmd.ClientID) > 128 || cmd.ClientID == "" || cmd.Sequence == 0 {
				c.JSON(400, gin.H{"error": "invalid_key_value_or_session"})
				return
			}
			n.execute(c, cmd)
		}
	}
	r.PUT("/v1/kv/:key", write("put"))
	r.DELETE("/v1/kv/:key", write("delete"))
	r.POST("/v1/kv/:key/cas", write("cas"))
	r.POST("/v1/admin/snapshot", func(c *gin.Context) {
		err := n.raft.Snapshot().Error()
		if err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
			c.JSON(503, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": "snapshot_complete"})
	})
	return r
}
