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

	"gopkg.in/yaml.v3"

	"github.com/han0110/provoor/pkg/cluster"
)

// Config describes one OpenVM cluster deployment.
type Config struct {
	// Zkvm names the backend and must be openvm.
	Zkvm string `yaml:"zkvm"`
	// Image and ImageTag name the cluster image. The artifacts volume is
	// named after the tag, so hosts cache one keyset per release.
	Image    string `yaml:"image"`
	ImageTag string `yaml:"image_tag"`
	// Verbose raises container log levels, 0 info, 1 debug, 2 trace.
	Verbose int `yaml:"verbose"`
	// Guests lists the guest ELF sources, local paths or URLs, provisioned
	// as the deployment's program loadout. Program names are content
	// digests, so a serve-side ELF must be byte-identical to its entry here.
	Guests      []string     `yaml:"guests"`
	Coordinator cluster.Node `yaml:"coordinator"`
	// Workers list one entry per worker container, each owning one GPU of
	// its host, so any topology is spelled out explicitly. A worker's
	// position in the list is its cluster-wide prover id.
	Workers []Worker     `yaml:"workers"`
	Config  ProverConfig `yaml:"config"`
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
	// SegmentMemory sets the worker's default segment memory in bytes,
	// left to the image default when zero.
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

func applyDefaults(cfg *Config) {
	if cfg.Image == "" {
		cfg.Image = "ghcr.io/han0110/openvm"
	}
	if cfg.ImageTag == "" {
		cfg.ImageTag = "2.1.0-preview"
	}
	nodes := []*cluster.Node{&cfg.Coordinator}
	for i := range cfg.Workers {
		nodes = append(nodes, &cfg.Workers[i].Node)
	}
	cluster.ApplyNodeDefaults(nodes...)
	if cfg.Config.AppProvers == 0 {
		cfg.Config.AppProvers = 2
	}
	if cfg.Config.LeafProvers == 0 {
		cfg.Config.LeafProvers = 2
	}
	if cfg.Config.InternalProvers == 0 {
		cfg.Config.InternalProvers = 1
	}
	if cfg.Config.TimeoutSecs == 0 {
		cfg.Config.TimeoutSecs = 1800
	}
	if cfg.Config.ShmSizeGB == 0 {
		cfg.Config.ShmSizeGB = 2
	}
}

func validate(cfg *Config) error {
	if cfg.Zkvm != "openvm" {
		return fmt.Errorf("zkvm %q is not openvm", cfg.Zkvm)
	}
	if cfg.Verbose < 0 || cfg.Verbose > 2 {
		return fmt.Errorf("verbose %d is out of range 0 to 2", cfg.Verbose)
	}
	if len(cfg.Guests) == 0 {
		return fmt.Errorf("at least one guest ELF source is required")
	}
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}
	seenGPU := map[string]bool{}
	seenWorkerURL := map[string]bool{}
	for _, worker := range cfg.Workers {
		if err := cluster.ValidateColocation(cfg.Coordinator, worker.Node); err != nil {
			return err
		}
		// The coordinator dials every worker back at its advertised URL, so
		// a worker off the coordinator host needs an address, the reverse
		// of the coordinator ip rule.
		if worker.Name != cfg.Coordinator.Name && worker.IP == "" {
			return fmt.Errorf("worker %q gpu %d is not co-located with the coordinator, worker ip is required", worker.Name, worker.GPU)
		}
		if worker.GPU < 0 {
			return fmt.Errorf("worker %q gpu expects a device id, got %d", worker.Name, worker.GPU)
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
			return fmt.Errorf("worker %q gpu %d advertises %s, already advertised by another worker", worker.Name, worker.GPU, advertised)
		}
		seenWorkerURL[advertised] = true
	}

	for name, value := range map[string]int{
		"app_provers":      cfg.Config.AppProvers,
		"leaf_provers":     cfg.Config.LeafProvers,
		"internal_provers": cfg.Config.InternalProvers,
		"timeout_secs":     cfg.Config.TimeoutSecs,
		"shm_size_gb":      cfg.Config.ShmSizeGB,
	} {
		if value < 1 {
			return fmt.Errorf("%s expects a positive integer, got %d", name, value)
		}
	}
	if cfg.Config.SegmentMemory < 0 {
		return fmt.Errorf("segment_memory expects a non-negative byte count, got %d", cfg.Config.SegmentMemory)
	}
	return nil
}
