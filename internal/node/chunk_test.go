package node

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// TestHandlerRoutesChunkID 验证带 chunk ID 的路径能够进入 chunk handler。
func TestHandlerRoutesChunkID(t *testing.T) {
	node, err := New("node-a", []string{"node-a"}, filepath.Join(t.TempDir(), "node.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	id := "not-a-valid-chunk-id"
	req := httptest.NewRequest(http.MethodGet, "/internal/chunks/"+id, nil)
	resp := httptest.NewRecorder()
	node.Handler().ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("response status = %d, want %d", resp.Code, http.StatusInternalServerError)
	}
}
