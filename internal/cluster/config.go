// Package cluster holds what every zkVM proving cluster shares. The
// configuration primitives, the Docker daemons reached over SSH, the
// container helpers, the telemetry sidecars, and the guest artifacts.
package cluster

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Sidecar kinds, named after the exporter each one runs.
const (
	sidecarDCGM = "dcgm-exporter"
	sidecarNode = "node-exporter"
)

// CoordinatorName labels the coordinator in progress lines.
const CoordinatorName = "coordinator"

// Config is the part of a cluster configuration every zkVM shares. Each zkVM
// package embeds it inline and adds its workers and prover settings.
type Config struct {
	Zkvm string `yaml:"zkvm"`
	// ZkvmVersion is the zkVM release the deployment proves under. It names
	// the volumes a host caches artifacts in, so an image rebuilt under
	// another tag shares them.
	ZkvmVersion string `yaml:"zkvm_version"`
	Image       string `yaml:"image"`
	ImageTag    string `yaml:"image_tag"`
	// Verbose raises container log levels, 0 info, 1 debug, 2 trace.
	Verbose     int       `yaml:"verbose"`
	Guests      []Guest   `yaml:"guests"`
	Coordinator Node      `yaml:"coordinator"`
	Telemetry   Telemetry `yaml:"telemetry"`
}

// Node identifies one deployment host by the destination the local ssh binary
// resolves, empty for the local daemon, and the address other cluster nodes
// dial. Entries sharing an SSH destination are co-located and dial each other
// over loopback.
type Node struct {
	SSH string `yaml:"ssh"`
	IP  string `yaml:"ip"`
}

// GPU selects the GPUs one container is given by their device ids. The
// selection stays an object, so a later key can join it.
type GPU struct {
	DeviceIDs []int `yaml:"device_ids"`
}

// Guest names a guest program's ELF and the verifying key its proofs are
// checked against. Both are a local path or an http(s) URL.
type Guest struct {
	ELF string `yaml:"elf"`
	VK  string `yaml:"vk"`
}

// Telemetry lists the metric sidecars a deployment runs, one entry per host
// and kind. An empty list runs none.
type Telemetry struct {
	// IntervalMs is the DCGM sampling period. DCGM refreshes profiling fields
	// at 10 Hz, so a shorter period only stores duplicates.
	IntervalMs int       `yaml:"interval_ms"`
	Sidecars   []Sidecar `yaml:"sidecars"`
}

// Sidecar is one exporter on one host.
type Sidecar struct {
	SSH  string `yaml:"ssh"`
	Kind string `yaml:"kind"`
}

// Zkvm reads the zkvm key of a configuration file, which selects the package
// that decodes the rest.
func Zkvm(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var peek struct {
		Zkvm string `yaml:"zkvm"`
	}
	if err := yaml.Unmarshal(raw, &peek); err != nil {
		return "", fmt.Errorf("parsing %s: %w", path, err)
	}
	return peek.Zkvm, nil
}

// Decode reads a configuration file into cfg, rejecting unknown keys.
func Decode(path string, cfg any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}
	return nil
}

// SetDefaults fills the image reference from the zkVM and its release.
func (c *Config) SetDefaults() {
	if c.Image == "" {
		c.Image = "ghcr.io/han0110/provoor/" + c.Zkvm
	}
	if c.ImageTag == "" {
		c.ImageTag = c.ZkvmVersion
	}
}

// Validate checks the shared fields. The zkVM package validates its workers
// and the telemetry sidecars against the hosts they name.
func (c *Config) Validate(zkvm string) error {
	if c.Zkvm != zkvm {
		return fmt.Errorf("zkvm %q is not %s", c.Zkvm, zkvm)
	}
	if c.ZkvmVersion == "" {
		return fmt.Errorf("zkvm_version is required")
	}
	if c.Verbose < 0 || c.Verbose > 2 {
		return fmt.Errorf("verbose %d is out of range 0 to 2", c.Verbose)
	}
	if len(c.Guests) == 0 {
		return fmt.Errorf("at least one guest is required")
	}
	for i, guest := range c.Guests {
		if guest.ELF == "" {
			return fmt.Errorf("guest %d is missing elf", i)
		}
		if guest.VK == "" {
			return fmt.Errorf("guest %d is missing vk", i)
		}
	}
	return nil
}

// ImageRef is the cluster image with its tag.
func (c *Config) ImageRef() string {
	return c.Image + ":" + c.ImageTag
}

// CoordinatorHost names the machine the coordinator runs on.
func (c *Config) CoordinatorHost() string {
	return HostName(c.Coordinator.SSH)
}

// ValidateColocation requires the coordinator's address once a worker lives
// on another host.
func ValidateColocation(coordinator, worker Node) error {
	if worker.SSH != coordinator.SSH && coordinator.IP == "" {
		return fmt.Errorf("worker on %s is not co-located with the coordinator, coordinator ip is required", HostName(worker.SSH))
	}
	return nil
}

// Len is the number of GPUs the selection exposes.
func (g GPU) Len() int {
	return len(g.DeviceIDs)
}

// label names the selection by its device ids, as 0_1.
func (g GPU) label() string {
	ids := make([]string, len(g.DeviceIDs))
	for i, id := range g.DeviceIDs {
		ids[i] = strconv.Itoa(id)
	}
	return strings.Join(ids, "_")
}

// Validate rejects a selection naming no device, a negative id, or a repeated
// one.
func (g GPU) Validate() error {
	if len(g.DeviceIDs) == 0 {
		return fmt.Errorf("gpu device_ids is required")
	}
	seen := map[int]bool{}
	for _, id := range g.DeviceIDs {
		if id < 0 {
			return fmt.Errorf("gpu device_ids expects non-negative ids, got %d", id)
		}
		if seen[id] {
			return fmt.Errorf("gpu device_ids repeats %d", id)
		}
		seen[id] = true
	}
	return nil
}

// interval is the DCGM sampling period, 100 ms unless configured.
func (t Telemetry) interval() time.Duration {
	if t.IntervalMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(t.IntervalMs) * time.Millisecond
}

// Validate rejects an unknown kind, a repeated sidecar, or a host the
// deployment does not dial.
func (t Telemetry) Validate(destinations []string) error {
	dialed := map[string]bool{}
	for _, destination := range destinations {
		dialed[destination] = true
	}
	seen := map[Sidecar]bool{}
	for i, sidecar := range t.Sidecars {
		if sidecar.Kind != sidecarDCGM && sidecar.Kind != sidecarNode {
			return fmt.Errorf("telemetry sidecar %d kind %q is not %s or %s", i, sidecar.Kind, sidecarDCGM, sidecarNode)
		}
		if !dialed[sidecar.SSH] {
			return fmt.Errorf("telemetry sidecar %d names host %q, which runs no coordinator or worker", i, sidecar.SSH)
		}
		if seen[sidecar] {
			return fmt.Errorf("telemetry sidecar %s on host %q repeats", sidecar.Kind, sidecar.SSH)
		}
		seen[sidecar] = true
	}
	return nil
}

// WorkerNameFormat shapes a worker name from its index and its GPU label.
const WorkerNameFormat = "worker_%d-gpu_%s"

// WorkerName identifies a worker by its position in the configuration and
// the GPUs it owns. Workers register under it and progress lines carry it.
func WorkerName(index int, gpu GPU) string {
	return fmt.Sprintf(WorkerNameFormat, index, gpu.label())
}
