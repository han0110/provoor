package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

// ResolveSource reads a guest artifact from a local path or an http(s) URL.
func ResolveSource(ctx context.Context, source string) ([]byte, error) {
	if !isURL(source) {
		return os.ReadFile(source)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", source, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching %s: status %s", source, resp.Status)
	}
	contents, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", source, err)
	}
	return contents, nil
}

// ResolveGuest reads a guest's ELF and verifying key.
func ResolveGuest(ctx context.Context, guest Guest) (elf, vk []byte, err error) {
	if elf, err = ResolveSource(ctx, guest.ELF); err != nil {
		return nil, nil, err
	}
	if vk, err = ResolveSource(ctx, guest.VK); err != nil {
		return nil, nil, err
	}
	return elf, vk, nil
}

// GuestELFName is a source's base name without the .elf extension. A signed
// asset URL carries its token in the query, which is not part of the name.
func GuestELFName(source string) string {
	if isURL(source) {
		if parsed, err := url.Parse(source); err == nil {
			source = parsed.Path
		}
	}
	return strings.TrimSuffix(path.Base(source), ".elf")
}

func isURL(source string) bool {
	return strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://")
}
