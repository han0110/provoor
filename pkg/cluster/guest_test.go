package cluster

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveGuestELFLocalPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stateless-validator-ethrex-zisk-v1.0.0-alpha.elf")
	if err := os.WriteFile(path, []byte("elf-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	elf, err := ResolveGuestELF(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(elf, []byte("elf-bytes")) {
		t.Errorf("elf = %q", elf)
	}
}

func TestResolveGuestELFMissingFile(t *testing.T) {
	if _, err := ResolveGuestELF(context.Background(), filepath.Join(t.TempDir(), "absent.elf")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestResolveGuestELFURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/release/guest.elf" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("elf-bytes"))
	}))
	defer server.Close()

	elf, err := ResolveGuestELF(context.Background(), server.URL+"/release/guest.elf")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(elf, []byte("elf-bytes")) {
		t.Errorf("elf = %q", elf)
	}

	if _, err := ResolveGuestELF(context.Background(), server.URL+"/absent.elf"); err == nil {
		t.Fatal("expected an error for a 404 response")
	}
}

func TestGuestELFName(t *testing.T) {
	for source, want := range map[string]string{
		"/guests/stateless-validator-ethrex-zisk-v1.0.0-alpha.elf":                    "stateless-validator-ethrex-zisk-v1.0.0-alpha",
		"https://example.com/download/stateless-validator-reth-openvm-v2.1.0-preview": "stateless-validator-reth-openvm-v2.1.0-preview",
	} {
		if got := GuestELFName(source); got != want {
			t.Errorf("GuestELFName(%q) = %q, want %q", source, got, want)
		}
	}
}

func TestSdkVersionFromELFName(t *testing.T) {
	for _, tt := range []struct {
		source, zkvm, want string
	}{
		{"/guests/stateless-validator-ethrex-zisk-v1.0.0-alpha.elf", "zisk", "1.0.0-alpha"},
		{"https://example.com/stateless-validator-reth-openvm-v2.1.0-preview.elf", "openvm", "2.1.0-preview"},
		{"/guests/custom.elf", "zisk", ""},
	} {
		if got := SdkVersionFromELFName(tt.source, tt.zkvm); got != tt.want {
			t.Errorf("SdkVersionFromELFName(%q, %q) = %q, want %q", tt.source, tt.zkvm, got, tt.want)
		}
	}
}
