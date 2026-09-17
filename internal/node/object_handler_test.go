package node

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleObjectDelete 验证对象 DELETE 删除 metadata 后 GET 返回 404。
func TestHandleObjectDelete(t *testing.T) {
	node := newReplicationTestNode(t)

	putReq := httptest.NewRequest(http.MethodPut, "/objects/file.txt", strings.NewReader("object data"))
	putResp := httptest.NewRecorder()
	node.Handler().ServeHTTP(putResp, putReq)
	if putResp.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d", putResp.Code, http.StatusOK)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/objects/file.txt", nil)
	deleteResp := httptest.NewRecorder()
	node.Handler().ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", deleteResp.Code, http.StatusNoContent)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/objects/file.txt", nil)
	getResp := httptest.NewRecorder()
	node.Handler().ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d, want %d", getResp.Code, http.StatusNotFound)
	}
}
