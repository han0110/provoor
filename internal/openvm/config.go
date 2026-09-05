// Package openvm deploys an OpenVM proving cluster and proves against it. An
// edge-manager coordinator serves the client API and one edge-worker per
// configured GPU registers against it. The program loadout is fixed at deploy
// time, so the coordinator refuses proofs for any other program.
package openvm

import (
	"fmt"
	"time"

	"github.com/han0110/provoor/internal/cluster"
)

// Config describes one OpenVM cluster deployment.
type Config struct {
	cluster.Config `yaml:",inline"`
	// Workers list one container per entry, each owning one GPU of its host.
	// A worker's position in the list is its cluster-wide prover id.
	Workers []Worker     `yaml:"workers"`
	Prover  ProverConfig `yaml:"config"`
}

// Worker is one worker container on the named host, owning one GPU.
type Worker struct {
	cluster.Node `yaml:",inline"`
	// GPU selects the one device the container owns. Its id names the
	// container and its port, so it is unique per host.
	GPU cluster.GPU `yaml:"gpu"`
}

// deviceID is the GPU the worker owns.
func (w Worker) deviceID() int {
	return w.GPU.DeviceIDs[0]
}

// ProverConfig applies across the deployment. The prover capacities reach
// both the manager and the workers, which the cluster requires to agree.
type ProverConfig struct {
	AppProvers      int `yaml:"app_provers"`
	LeafProvers     int `yaml:"leaf_provers"`
	InternalProvers int `yaml:"internal_provers"`
	// SegmentMemory is the worker's default segment memory in bytes. Left to
	// the VM, two concurrent app provers can exhaust a 32 GB card.
	SegmentMemory int64 `yaml:"segment_memory"`
	// TimeoutSecs is the manager's watchdog deadline per proof.
	TimeoutSecs int `yaml:"timeout_secs"`
	// ShmSizeGB sizes /dev/shm of the worker and keygen containers.
	ShmSizeGB int `yaml:"shm_size_gb"`
}

// Load reads, defaults, and validates an OpenVM cluster configuration file.
func Load(path string) (*Config, error) {
	cfg := &Config{}
	if err := cluster.Decode(path, cfg); err != nil {
		return nil, err
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return cfg, nil
}

// setDefaults fills what the image cannot fall back on. The manager carries no
// fallback for the prover capacities, the segment memory has none upstream,
// and the image's own proof deadline is shorter than the forwarder's budget.
func (cfg *Config) setDefaults() {
	cfg.Config.SetDefaults()
	if cfg.Prover.AppProvers == 0 {
		cfg.Prover.AppProvers = 2
	}
	if cfg.Prover.LeafProvers == 0 {
		cfg.Prover.LeafProvers = 2
	}
	if cfg.Prover.InternalProvers == 0 {
		cfg.Prover.InternalProvers = 1
	}
	if cfg.Prover.ShmSizeGB == 0 {
		cfg.Prover.ShmSizeGB = 2
	}
	if cfg.Prover.SegmentMemory == 0 {
		cfg.Prover.SegmentMemory = 15 << 30
	}
	if cfg.Prover.TimeoutSecs == 0 {
		cfg.Prover.TimeoutSecs = int(cluster.DefaultProveTimeout / time.Second)
	}
}

func (cfg *Config) validate() error {
	if err := cfg.Config.Validate("openvm"); err != nil {
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
		if err := worker.GPU.Validate(); err != nil {
			return fmt.Errorf("worker %d: %w", i, err)
		}
		// The device id names the container and its port, so the selection
		// is one explicit id.
		if len(worker.GPU.DeviceIDs) != 1 {
			return fmt.Errorf("worker %d gpu device_ids expects exactly one id, got %v", i, worker.GPU.DeviceIDs)
		}
		if err := cluster.ValidateColocation(cfg.Coordinator, worker.Node); err != nil {
			return err
		}
		// The coordinator dials every worker back at its advertised URL, so
		// a worker off the coordinator host needs an address too.
		if worker.SSH != cfg.Coordinator.SSH && worker.IP == "" {
			return fmt.Errorf("%s is not co-located with the coordinator, worker ip is required", workerName(i, worker))
		}
		key := fmt.Sprintf("%s/%d", worker.SSH, worker.deviceID())
		if seenGPU[key] {
			return fmt.Errorf("duplicate worker gpu %d on host %q", worker.deviceID(), worker.SSH)
		}
		seenGPU[key] = true
		// Two entries resolving to one URL would silently share a process.
		advertised := workerURL(worker)
		if seenWorkerURL[advertised] {
			return fmt.Errorf("%s advertises %s, already advertised by another worker", workerName(i, worker), advertised)
		}
		seenWorkerURL[advertised] = true
	}

	for name, value := range map[string]int{
		"app_provers":      cfg.Prover.AppProvers,
		"leaf_provers":     cfg.Prover.LeafProvers,
		"internal_provers": cfg.Prover.InternalProvers,
		"shm_size_gb":      cfg.Prover.ShmSizeGB,
	} {
		if value < 1 {
			return fmt.Errorf("%s expects a positive integer, got %d", name, value)
		}
	}
	if cfg.Prover.SegmentMemory < 0 {
		return fmt.Errorf("segment_memory expects a non-negative byte count, got %d", cfg.Prover.SegmentMemory)
	}
	if cfg.Prover.TimeoutSecs < 0 {
		return fmt.Errorf("timeout_secs expects a non-negative integer, got %d", cfg.Prover.TimeoutSecs)
	}
	return nil
}

func (cfg *Config) destinations() []string {
	destinations := []string{cfg.Coordinator.SSH}
	for _, worker := range cfg.Workers {
		destinations = append(destinations, worker.SSH)
	}
	return destinations
}
