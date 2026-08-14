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

func TestResolveSourceLocalPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stateless-validator-ethrex-zisk-v1.0.0-alpha.elf")
	if err := os.WriteFile(path, []byte("elf-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	elf, err := ResolveSource(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(elf, []byte("elf-bytes")) {
		t.Errorf("elf = %q", elf)
	}
}

func TestResolveSourceMissingFile(t *testing.T) {
	if _, err := ResolveSource(context.Background(), filepath.Join(t.TempDir(), "absent.elf")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestResolveSourceURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/release/guest.elf" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("elf-bytes"))
	}))
	defer server.Close()

	elf, err := ResolveSource(context.Background(), server.URL+"/release/guest.elf")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(elf, []byte("elf-bytes")) {
		t.Errorf("elf = %q", elf)
	}

	if _, err := ResolveSource(context.Background(), server.URL+"/absent.elf"); err == nil {
		t.Fatal("expected an error for a 404 response")
	}
}

func TestResolveGuest(t *testing.T) {
	dir := t.TempDir()
	elfPath := filepath.Join(dir, "guest.elf")
	vkPath := filepath.Join(dir, "guest.vk")
	if err := os.WriteFile(elfPath, []byte("elf-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(vkPath, []byte("vk-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	elf, verifyingKey, err := ResolveGuest(context.Background(), Guest{ELF: elfPath, VK: vkPath})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(elf, []byte("elf-bytes")) || !bytes.Equal(verifyingKey, []byte("vk-bytes")) {
		t.Errorf("elf = %q, vk = %q", elf, verifyingKey)
	}

	// A guest is unusable without both, so an absent key fails the pair
	// rather than yielding an ELF a caller could go on to prove.
	if _, _, err := ResolveGuest(context.Background(), Guest{ELF: elfPath, VK: filepath.Join(dir, "absent.vk")}); err == nil {
		t.Error("expected an error for a missing vk")
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
