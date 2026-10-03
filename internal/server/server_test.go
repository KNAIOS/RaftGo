package server

import (
	"net/http/httptest"
	"testing"
)

func TestAuthenticationAndValidation(t *testing.T) {
	// No Raft node is needed for requests rejected by middleware or validation.
	n := &Node{cfg: Config{ID: "test", Token: "secret"}}
	handler := n.Router()
	cases := []struct {
		method, path, token string
		want                int
	}{
		{"GET", "/healthz", "", 200},
		{"GET", "/v1/status", "", 401},
		{"PUT", "/v1/kv/k", "Bearer secret", 400},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", tc.token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s %s got=%d want=%d", tc.method, tc.path, rec.Code, tc.want)
		}
	}
}
func TestConfigValidation(t *testing.T) {
	t.Setenv("NODE_ID", "n1")
	t.Setenv("RAFT_ADVERTISE", "127.0.0.1:7000")
	t.Setenv("PEERS", `[{"id":"n1","raft":"127.0.0.1:7000","api":"http://127.0.0.1:8080"},{"id":"n1","raft":"127.0.0.1:7001","api":"http://127.0.0.1:8081"}]`)
	if _, err := FromEnv(); err == nil {
		t.Fatal("duplicate identity accepted")
	}
}
