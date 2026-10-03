package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

const MaxSessions = 10000
const MaxKVBytes = 16 << 20
const MaxKeys = 100000

type Command struct {
	Op             string `json:"op"`
	Key            string `json:"key"`
	Value          string `json:"value,omitempty"`
	Expected       string `json:"expected,omitempty"`
	ExpectedExists bool   `json:"expected_exists,omitempty"`
	ClientID       string `json:"client_id,omitempty"`
	Sequence       uint64 `json:"sequence,omitempty"`
}
type Result struct {
	Value   string `json:"value"`
	Exists  bool   `json:"exists"`
	Applied bool   `json:"applied"`
	Error   string `json:"error,omitempty"`
}
type Session struct {
	Sequence uint64   `json:"sequence"`
	Hash     [32]byte `json:"hash"`
	Result   Result   `json:"result"`
}
type State struct {
	KV       map[string]string  `json:"kv"`
	Sessions map[string]Session `json:"sessions"`
	Bytes    int                `json:"bytes"`
}
type FSM struct {
	mu    sync.RWMutex
	state State
}

func New() *FSM { return &FSM{state: State{KV: map[string]string{}, Sessions: map[string]Session{}}} }
func (f *FSM) Apply(entry *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return Result{Error: "invalid_command"}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd.Op == "get" {
		v, ok := f.state.KV[cmd.Key]
		return Result{Value: v, Exists: ok}
	}
	if cmd.Op != "put" && cmd.Op != "delete" && cmd.Op != "cas" {
		return Result{Error: "invalid_operation"}
	}
	if cmd.ClientID == "" || cmd.Sequence == 0 {
		return Result{Error: "invalid_session"}
	}
	hash := sha256.Sum256(entry.Data)
	previous, known := f.state.Sessions[cmd.ClientID]
	if known {
		if cmd.Sequence < previous.Sequence {
			return Result{Error: "stale_sequence"}
		}
		if cmd.Sequence == previous.Sequence {
			if hash != previous.Hash {
				return Result{Error: "sequence_conflict"}
			}
			return previous.Result
		}
	} else if len(f.state.Sessions) >= MaxSessions {
		return Result{Error: "session_limit"}
	}
	value, exists := f.state.KV[cmd.Key]
	result := Result{Value: value, Exists: exists}
	newBytes := f.state.Bytes
	if cmd.Op == "put" || (cmd.Op == "cas" && exists == cmd.ExpectedExists && (!exists || value == cmd.Expected)) {
		if exists {
			newBytes -= len(cmd.Key) + len(value)
		}
		newBytes += len(cmd.Key) + len(cmd.Value)
		if newBytes > MaxKVBytes || (!exists && len(f.state.KV) >= MaxKeys) {
			result.Error = "store_full"
			f.state.Sessions[cmd.ClientID] = Session{Sequence: cmd.Sequence, Hash: hash, Result: result}
			return result
		}
	}
	switch cmd.Op {
	case "put":
		f.state.KV[cmd.Key] = cmd.Value
		f.state.Bytes = newBytes
		result = Result{Value: cmd.Value, Exists: true, Applied: true}
	case "delete":
		delete(f.state.KV, cmd.Key)
		if exists {
			f.state.Bytes -= len(cmd.Key) + len(value)
		}
		result.Applied = true
	case "cas":
		if exists == cmd.ExpectedExists && (!exists || value == cmd.Expected) {
			f.state.KV[cmd.Key] = cmd.Value
			f.state.Bytes = newBytes
			result = Result{Value: cmd.Value, Exists: true, Applied: true}
		}
	}
	f.state.Sessions[cmd.ClientID] = Session{Sequence: cmd.Sequence, Hash: hash, Result: result}
	return result
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return &snapshot{data}, nil
}
func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var state State
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return err
	}
	if state.KV == nil || state.Sessions == nil {
		return fmt.Errorf("invalid snapshot")
	}
	state.Bytes = 0
	for key, value := range state.KV {
		state.Bytes += len(key) + len(value)
	}
	if state.Bytes > MaxKVBytes || len(state.KV) > MaxKeys || len(state.Sessions) > MaxSessions {
		return fmt.Errorf("snapshot exceeds capacity")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	return nil
}

type snapshot struct{ data []byte }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *snapshot) Release() {}
