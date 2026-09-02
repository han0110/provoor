// Package openvm deploys an OpenVM proving cluster over Docker daemons
// reached through SSH, an edge-manager coordinator serving the client API
// plus one edge-worker container per configured GPU, each worker registering
// against the coordinator. The program loadout is fixed at deploy time,
// every host derives a proving keyset from the guest ELFs into a shared
// volume, and the coordinator refuses proofs for any other program.
package openvm

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/serve"
)

// defaultTimeoutSecs is the manager's watchdog deadline per proof, over the
// 300 the image defaults to. It tracks the forwarder's own budget, so the
// cluster does not fail a proof the forwarder is still waiting on.
const defaultTimeoutSecs = int(serve.DefaultProveTimeout / time.Second)

// Config describes one OpenVM cluster deployment.
type Config struct {
	// Zkvm names the backend and must be openvm.
	Zkvm string `yaml:"zkvm"`
	// ZkvmVersion is the OpenVM release the deployment proves under. It names
	// the volumes a host caches its keysets in, so an image rebuilt under
	// another tag still shares the artifacts of its release.
	ZkvmVersion string `yaml:"zkvm_version"`
	// Image and ImageTag name the cluster image, the tag defaulting to the
	// OpenVM release it carries.
	Image    string `yaml:"image"`
	ImageTag string `yaml:"image_tag"`
	// Verbose raises container log levels, 0 info, 1 debug, 2 trace.
	Verbose int `yaml:"verbose"`
	// Guests lists the guest programs provisioned as the deployment's
	// program loadout, each an ELF and its verifying key sourced from a
	// local path or a URL. Program names are content digests, so a
	// serve-side ELF must be byte-identical to its entry here.
	Guests      []cluster.Guest `yaml:"guests"`
	Coordinator cluster.Node    `yaml:"coordinator"`
	// Workers list one entry per worker container, each owning one GPU of
	// its host, so any topology is spelled out explicitly. A worker's
	// position in the list is its cluster-wide prover id.
	Workers []Worker `yaml:"workers"`
	// Telemetry lists the metric sidecars, one per host and exporter kind.
	Telemetry cluster.Telemetry `yaml:"telemetry"`
	Config    ProverConfig      `yaml:"config"`
}

// Worker is one worker container, on the named host, owning one GPU.
type Worker struct {
	cluster.Node `yaml:",inline"`
	// GPU is the device the container owns, also naming the container and
	// its port, so it must be unique per host.
	GPU int `yaml:"gpu"`
}

// ProverConfig applies across the deployment. The prover capacity fields go
// into both the manager and worker configurations, which the cluster
// requires to agree for a worker registration to be accepted.
type ProverConfig struct {
	// AppProvers, LeafProvers, and InternalProvers cap each worker's
	// concurrent provers per proving stage.
	AppProvers      int `yaml:"app_provers"`
	LeafProvers     int `yaml:"leaf_provers"`
	InternalProvers int `yaml:"internal_provers"`
	// SegmentMemory sets the worker's default segment memory in bytes.
	// Upstream leaves it unset and lets the VM pick, which on a 32 GB card
	// two concurrent app provers can exhaust, so a deployment names a figure
	// and this one defaults to 15 GiB.
	SegmentMemory int64 `yaml:"segment_memory"`
	// TimeoutSecs is the manager's watchdog deadline per proof.
	TimeoutSecs int `yaml:"timeout_secs"`
	// ShmSizeGB sizes /dev/shm of the worker and keygen containers.
	ShmSizeGB int `yaml:"shm_size_gb"`
}

// Load reads, defaults, and validates an OpenVM cluster configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	cfg := &Config{}
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	applyDefaults(cfg)
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return cfg, nil
}

// applyDefaults fills in only what cannot be left out. The prover capacities
// are mandatory because the manager's own configuration carries no fallback
// for them, unlike the worker's, and rejects a worker whose capacities differ,
// so both files name the figures the image's deployment defaults carry. The
// segment memory has no upstream default at all. Every other knob stays at its
// zero value when unset and leaves the rendered file, so the image applies its
// own default rather than one this repository would have to track.
func applyDefaults(cfg *Config) {
	if cfg.Image == "" {
		cfg.Image = "ghcr.io/han0110/provoor/openvm"
	}
	if cfg.ImageTag == "" {
		cfg.ImageTag = cfg.ZkvmVersion
	}
	if cfg.Config.AppProvers == 0 {
		cfg.Config.AppProvers = 2
	}
	if cfg.Config.LeafProvers == 0 {
		cfg.Config.LeafProvers = 2
	}
	if cfg.Config.InternalProvers == 0 {
		cfg.Config.InternalProvers = 1
	}
	if cfg.Config.ShmSizeGB == 0 {
		cfg.Config.ShmSizeGB = 2
	}
	if cfg.Config.SegmentMemory == 0 {
		cfg.Config.SegmentMemory = 15 << 30
	}
	if cfg.Config.TimeoutSecs == 0 {
		cfg.Config.TimeoutSecs = defaultTimeoutSecs
	}
}

func validate(cfg *Config) error {
	if cfg.Zkvm != "openvm" {
		return fmt.Errorf("zkvm %q is not openvm", cfg.Zkvm)
	}
	if cfg.ZkvmVersion == "" {
		return fmt.Errorf("zkvm_version is required, the OpenVM release the deployment proves under")
	}
	if cfg.Verbose < 0 || cfg.Verbose > 2 {
		return fmt.Errorf("verbose %d is out of range 0 to 2", cfg.Verbose)
	}
	if err := cluster.ValidateGuests(cfg.Guests); err != nil {
		return err
	}
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}
	if err := cfg.Telemetry.Validate(cfg.destinations()); err != nil {
		return err
	}
	seenGPU := map[string]bool{}
	seenWorkerURL := map[string]bool{}
	for i, worker := range cfg.Workers {
		if err := cluster.ValidateColocation(cfg.Coordinator, worker.Node); err != nil {
			return err
		}
		// The coordinator dials every worker back at its advertised URL, so
		// a worker off the coordinator host needs an address, the reverse
		// of the coordinator ip rule.
		if worker.SSH != cfg.Coordinator.SSH && worker.IP == "" {
			return fmt.Errorf("%s is not co-located with the coordinator, worker ip is required", workerName(i, worker))
		}
		if worker.GPU < 0 {
			return fmt.Errorf("worker %d gpu expects a device id, got %d", i, worker.GPU)
		}
		key := fmt.Sprintf("%s/%d", worker.SSH, worker.GPU)
		if seenGPU[key] {
			return fmt.Errorf("duplicate worker gpu %d on host %q", worker.GPU, worker.SSH)
		}
		seenGPU[key] = true
		// The manager dials workers at the address they advertise, so two
		// entries resolving to one url silently share a single process.
		advertised := workerURL(worker)
		if seenWorkerURL[advertised] {
			return fmt.Errorf("%s advertises %s, already advertised by another worker", workerName(i, worker), advertised)
		}
		seenWorkerURL[advertised] = true
	}

	for name, value := range map[string]int{
		"app_provers":      cfg.Config.AppProvers,
		"leaf_provers":     cfg.Config.LeafProvers,
		"internal_provers": cfg.Config.InternalProvers,
		"shm_size_gb":      cfg.Config.ShmSizeGB,
	} {
		if value < 1 {
			return fmt.Errorf("%s expects a positive integer, got %d", name, value)
		}
	}
	if cfg.Config.SegmentMemory < 0 {
		return fmt.Errorf("segment_memory expects a non-negative byte count, got %d", cfg.Config.SegmentMemory)
	}
	if cfg.Config.TimeoutSecs < 0 {
		return fmt.Errorf("timeout_secs expects a non-negative integer, got %d", cfg.Config.TimeoutSecs)
	}
	return nil
}
