package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
)

// ResolveGuestELF reads a guest ELF from its source, a local file path or an
// http(s) URL such as an ere-guests release asset.
func ResolveGuestELF(ctx context.Context, source string) ([]byte, error) {
	if !strings.HasPrefix(source, "http://") && !strings.HasPrefix(source, "https://") {
		return os.ReadFile(source)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching guest ELF %s: %w", source, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching guest ELF %s: status %s", source, resp.Status)
	}
	elf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetching guest ELF %s: %w", source, err)
	}
	return elf, nil
}

// GuestELFName is a source's base name without the .elf extension, which for
// release assets follows stateless-validator-<guest>-<zkvm>-v<sdk-version>.
func GuestELFName(source string) string {
	return strings.TrimSuffix(path.Base(source), ".elf")
}

// SdkVersionFromELFName extracts the zkVM SDK version suffix from a
// conventionally named guest ELF source, empty when the name carries none.
func SdkVersionFromELFName(source, zkvm string) string {
	_, version, found := strings.Cut(GuestELFName(source), "-"+zkvm+"-v")
	if !found {
		return ""
	}
	return version
}
