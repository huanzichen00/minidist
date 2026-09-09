package node

import (
	"reflect"
	"testing"
)

func TestFailureDetectorMemberDoesNotBecomeRingMember(t *testing.T) {
	n := New("node-a", []string{"node-a", "node-b", "node-c"})
	n.fd.TrackMember("node-x")

	want := []string{"node-a", "node-b", "node-c"}
	if got := n.ring.Members(); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected ring members %v, got %v", want, got)
	}
}
