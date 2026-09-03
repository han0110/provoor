package cluster

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "guest.elf")
	if err := os.WriteFile(path, []byte("elf-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/release/guest.elf" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("elf-bytes"))
	}))
	defer server.Close()

	for _, source := range []string{path, server.URL + "/release/guest.elf"} {
		elf, err := ResolveSource(t.Context(), source)
		if err != nil || !bytes.Equal(elf, []byte("elf-bytes")) {
			t.Errorf("ResolveSource(%q) = %q, %v", source, elf, err)
		}
	}
	for _, source := range []string{filepath.Join(dir, "absent.elf"), server.URL + "/absent.elf"} {
		if _, err := ResolveSource(t.Context(), source); err == nil {
			t.Errorf("ResolveSource(%q) = nil, want an error", source)
		}
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
	elf, vk, err := ResolveGuest(t.Context(), Guest{ELF: elfPath, VK: vkPath})
	if err != nil || !bytes.Equal(elf, []byte("elf-bytes")) || !bytes.Equal(vk, []byte("vk-bytes")) {
		t.Errorf("elf = %q, vk = %q, err = %v", elf, vk, err)
	}
	if _, _, err := ResolveGuest(t.Context(), Guest{ELF: elfPath, VK: filepath.Join(dir, "absent.vk")}); err == nil {
		t.Error("expected an error for a missing vk")
	}
}

func TestGuestELFName(t *testing.T) {
	cases := map[string]string{
		"/guests/stateless-validator-ethrex-zisk-v1.0.0-alpha.elf":                    "stateless-validator-ethrex-zisk-v1.0.0-alpha",
		"https://example.com/download/stateless-validator-reth-openvm-v2.1.0-preview": "stateless-validator-reth-openvm-v2.1.0-preview",
		"https://example.com/guest.elf?token=abc":                                     "guest",
	}
	for source, want := range cases {
		if got := GuestELFName(source); got != want {
			t.Errorf("GuestELFName(%q) = %q, want %q", source, got, want)
		}
	}
}
