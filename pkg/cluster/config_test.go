package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestZkvm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte("zkvm: zisk\nunrelated: ignored\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	zkvm, err := Zkvm(path)
	if err != nil || zkvm != "zisk" {
		t.Errorf("Zkvm = %q, err = %v", zkvm, err)
	}
	if _, err := Zkvm(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("expected error for a missing file")
	}
}
