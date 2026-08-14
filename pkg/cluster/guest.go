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

// Guest names one guest program's artifacts, the ELF a cluster proves and the
// verifying key its proofs are checked against. An ere-guests release
// publishes the two side by side, and naming the key here rather than taking
// the cluster's own makes the guest, not the cluster, decide what a proof has
// to be about.
type Guest struct {
	ELF string `yaml:"elf"`
	VK  string `yaml:"vk"`
}

// ValidateGuests rejects a guest list a cluster cannot be deployed from,
// naming the entry at fault so a long list is legible.
func ValidateGuests(guests []Guest) error {
	if len(guests) == 0 {
		return fmt.Errorf("at least one guest is required")
	}
	for i, guest := range guests {
		if guest.ELF == "" {
			return fmt.Errorf("guest %d is missing elf", i)
		}
		if guest.VK == "" {
			return fmt.Errorf("guest %d is missing vk", i)
		}
	}
	return nil
}

// ResolveSource reads a guest artifact from its source, a local file path or
// an http(s) URL such as an ere-guests release asset.
func ResolveSource(ctx context.Context, source string) ([]byte, error) {
	if !strings.HasPrefix(source, "http://") && !strings.HasPrefix(source, "https://") {
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

// ResolveGuest reads a guest's ELF and verifying key from their sources.
func ResolveGuest(ctx context.Context, guest Guest) (elf, verifyingKey []byte, err error) {
	elf, err = ResolveSource(ctx, guest.ELF)
	if err != nil {
		return nil, nil, err
	}
	verifyingKey, err = ResolveSource(ctx, guest.VK)
	if err != nil {
		return nil, nil, err
	}
	return elf, verifyingKey, nil
}

// GuestELFName is a source's base name without the .elf extension, which for
// release assets follows stateless-validator-<guest>-<zkvm>-v<sdk-version>.
func GuestELFName(source string) string {
	// A signed asset URL carries its token in the query, which is not part of
	// the name reported as the client version.
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		if parsed, err := url.Parse(source); err == nil {
			source = parsed.Path
		}
	}
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
