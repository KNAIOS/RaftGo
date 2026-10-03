package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"strings"
	"sync"
	"testing"
)

func apply(f *FSM, c Command) Result {
	b, _ := json.Marshal(c)
	return f.Apply(&raft.Log{Data: b}).(Result)
}
func TestCASDedupSnapshot(t *testing.T) {
	f := New()
	put := Command{Op: "put", Key: "k", Value: "a", ClientID: "c", Sequence: 1}
	if !apply(f, put).Applied {
		t.Fatal("put failed")
	}
	cas := Command{Op: "cas", Key: "k", ExpectedExists: true, Expected: "a", Value: "b", ClientID: "c", Sequence: 2}
	first := apply(f, cas)
	if !first.Applied {
		t.Fatal("CAS failed")
	}
	if second := apply(f, cas); second != first {
		t.Fatal("duplicate CAS must return original result")
	}
	cas.Value = "different"
	if apply(f, cas).Error != "sequence_conflict" {
		t.Fatal("sequence reuse accepted")
	}
	if apply(f, put).Error != "stale_sequence" {
		t.Fatal("old request accepted")
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err = snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	// Mutating the FSM after taking a snapshot must not alter the captured state.
	apply(f, Command{Op: "put", Key: "k", Value: "later", ClientID: "c", Sequence: 3})
	restored := New()
	if err = restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if got := apply(restored, Command{Op: "get", Key: "k"}); got.Value != "b" {
		t.Fatalf("snapshot mismatch: %+v", got)
	}
	cas.Value = "b"
	if apply(restored, cas) != first {
		t.Fatal("snapshot lost dedup state")
	}
}
func TestCASMissingAndDelete(t *testing.T) {
	f := New()
	failed := Command{Op: "cas", Key: "k", Value: "v", ExpectedExists: true, Expected: "", ClientID: "c", Sequence: 1}
	if apply(f, failed).Applied {
		t.Fatal("missing key confused with empty value")
	}
	failed.ExpectedExists = false
	failed.Sequence = 2
	if !apply(f, failed).Applied {
		t.Fatal("create CAS failed")
	}
	if !apply(f, Command{Op: "delete", Key: "k", ClientID: "c", Sequence: 3}).Applied {
		t.Fatal("delete failed")
	}
	if apply(f, Command{Op: "get", Key: "k"}).Exists {
		t.Fatal("key not deleted")
	}
}
func TestConcurrentCAS(t *testing.T) {
	f := New()
	apply(f, Command{Op: "put", Key: "k", Value: "a", ClientID: "initial", Sequence: 1})
	var wg sync.WaitGroup
	results := make(chan Result, 2)
	for _, id := range []string{"one", "two"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			results <- apply(f, Command{Op: "cas", Key: "k", ExpectedExists: true, Expected: "a", Value: id, ClientID: id, Sequence: 1})
		}(id)
	}
	wg.Wait()
	close(results)
	success := 0
	for r := range results {
		if r.Applied {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("CAS winners=%d", success)
	}
}

type memorySink struct{ bytes.Buffer }

func TestCapacityAndReuse(t *testing.T) {
	f := New()
	value := strings.Repeat("x", 64<<10)
	for i := 0; i < 255; i++ {
		result := apply(f, Command{Op: "put", Key: fmt.Sprintf("k%d", i), Value: value, ClientID: "capacity", Sequence: uint64(i + 1)})
		if !result.Applied {
			t.Fatalf("unexpected capacity at %d: %+v", i, result)
		}
	}
	request := Command{Op: "put", Key: "overflow", Value: value, ClientID: "capacity", Sequence: 256}
	first := apply(f, request)
	if first.Error != "store_full" {
		t.Fatal("capacity not enforced")
	}
	apply(f, Command{Op: "delete", Key: "k0", ClientID: "capacity", Sequence: 257})
	request.Sequence = 258
	if !apply(f, request).Applied {
		t.Fatal("delete did not release capacity")
	}
	if f.state.Bytes > MaxKVBytes {
		t.Fatal("capacity accounting exceeded")
	}
}

func (s *memorySink) ID() string    { return "test" }
func (s *memorySink) Cancel() error { return nil }
func (s *memorySink) Close() error  { return nil }
