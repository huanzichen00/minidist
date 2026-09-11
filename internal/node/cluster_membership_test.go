package node

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFailureDetectorMemberDoesNotBecomeRingMember(t *testing.T) {
	n, err := New(
		"node-a",
		[]string{"node-a", "node-b", "node-c"},
		filepath.Join(t.TempDir(), "node.wal"),
	)
	if err != nil {
		t.Fatal(err)
	}
	n.fd.TrackMember("node-x")

	want := []string{"node-a", "node-b", "node-c"}
	if got := n.ring.Members(); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected ring members %v, got %v", want, got)
	}
}

func TestApplyClusterConfigCompensatesPartialSync(t *testing.T) {
	var applied []ClusterConfig

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var config ClusterConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			t.Fatal(err)
		}

		applied = append(applied, config)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer second.Close()

	n, err := New(
		"coordinator",
		[]string{"coordinator"},
		filepath.Join(t.TempDir(), "node.wal"),
	)
	if err != nil {
		t.Fatal(err)
	}
	previous := ClusterConfig{
		Members: []string{"node-a", "node-b"},
	}
	next := ClusterConfig{
		Version: 1,
		Members: []string{"node-a", "node-b", "node-c"},
	}

	err = n.applyClusterConfig(
		t.Context(),
		[]string{
			strings.TrimPrefix(first.URL, "http://"),
			strings.TrimPrefix(second.URL, "http://"),
		},
		previous,
		next,
	)
	if err == nil {
		t.Fatal("expected partial sync error")
	}

	want := []ClusterConfig{
		next,
		{
			Version: 2,
			Members: previous.Members,
		},
	}
	if !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied configs = %#v, want %#v", applied, want)
	}

	if got := n.configVersion.Load(); got != 2 {
		t.Fatalf("config version = %d, want 2", got)
	}
}
