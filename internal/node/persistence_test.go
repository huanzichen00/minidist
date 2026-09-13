package node

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotCompactsWALAndRestoresKeys(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node.wal")
	n, err := New("node-a", []string{"node-a"}, walPath)
	if err != nil {
		t.Fatal(err)
	}

	n.replicas = 1
	n.writeQuorum = 1
	n.readQuorum = 1

	server := httptest.NewServer(n.Handler())
	client := server.Client()

	for i := range 100 {
		key := fmt.Sprintf("key-%03d", i)
		body := []byte(fmt.Sprintf("value-%03d", i))
		put(t, client, server.URL+"/kv/"+key, body)
	}

	walInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if walInfo.Size() == 0 {
		t.Fatal("expected WAL to contain records before snapshot")
	}

	if err := n.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}

	snapshotPath := walPath + ".snapshot"
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file: %v", err)
	}

	walInfo, err = os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if walInfo.Size() != 0 {
		t.Fatalf("WAL size = %d, want 0", walInfo.Size())
	}

	server.Close()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := New("node-a", []string{"node-a"}, walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	restarted.replicas = 1
	restarted.writeQuorum = 1
	restarted.readQuorum = 1
	restartedServer := httptest.NewServer(restarted.Handler())
	defer restartedServer.Close()

	for i := range 100 {
		key := fmt.Sprintf("key-%03d", i)
		want := fmt.Sprintf("value-%03d", i)
		get(t, restartedServer.Client(), restartedServer.URL+"/kv/"+key, want)
	}
}

func put(t *testing.T, client *http.Client, url string, body []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT %s: status = %s", url, resp.Status)
	}
}

func get(t *testing.T, client *http.Client, url string, want string) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status = %s", url, resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("GET %s: body = %q, want %q", url, data, want)
	}
}
