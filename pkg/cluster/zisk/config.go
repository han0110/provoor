// Package zisk deploys a ZisK proving cluster over Docker daemons reached
// through SSH, a coordinator container serving the client API plus one GPU
// worker container per host, each worker registering against the coordinator.
package zisk

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/han0110/provoor/pkg/cluster"
)

// Config describes one ZisK cluster deployment.
type Config struct {
	// Zkvm names the backend and must be zisk.
	Zkvm string `yaml:"zkvm"`
	// Image and ImageTag name the cluster image. The proving-key volume is
	// named after the tag, so hosts cache one key set per release.
	Image    string `yaml:"image"`
	ImageTag string `yaml:"image_tag"`
	// Verbose raises container log levels, 0 info, 1 debug, 2 trace.
	Verbose     int          `yaml:"verbose"`
	Coordinator cluster.Node `yaml:"coordinator"`
	Workers     []Worker     `yaml:"workers"`
	Config      WorkerConfig `yaml:"config"`
}

// Worker is one GPU host, whose single worker container spans the GPUs it
// exposes.
type Worker struct {
	cluster.Node `yaml:",inline"`
	// Gpus exposes GPUs to the container, all, a count, or device=0,1.
	Gpus string `yaml:"gpus"`
}

// WorkerConfig applies to every worker. Each field maps onto one mpirun or
// zisk-worker-gpu flag of the worker invocation.
type WorkerConfig struct {
	// ShmSizeGB sizes /dev/shm for the ASM emulator's shared-memory trace
	// segments.
	ShmSizeGB int `yaml:"shm_size_gb"`
	// MPINp is the MPI rank count per worker.
	MPINp int `yaml:"mpi_np"`
	// MPINumaPpr is the rank count per NUMA node. Defaults to MPINp divided
	// by the host's NUMA node count, observed at deploy time, minimum 1.
	MPINumaPpr int `yaml:"mpi_numa_ppr"`
	// MPIThreads is RAYON_NUM_THREADS per rank. Unset leaves proofman's own
	// choice, physical cores divided by the node's rank count.
	MPIThreads int `yaml:"mpi_threads"`
	// MaxStreams caps GPU streams.
	MaxStreams int `yaml:"max_streams"`
	// NumberThreadsWitness sets witness computation threads.
	NumberThreadsWitness int `yaml:"number_threads_witness"`
	// MaxWitnessStored sizes the recursive witness buffer pools, which
	// default to proofman's 10 buffers. A pool of 4 frees roughly 10 GB of
	// heap at no measured proving-time cost.
	MaxWitnessStored int `yaml:"max_witness_stored"`
	// MinimalMemory builds collectors per instance instead of batched,
	// freeing roughly 11 GB of heap for step-heavy blocks.
	MinimalMemory bool `yaml:"minimal_memory"`
}

// Load reads, defaults, and validates a ZisK cluster configuration file.
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
		cfg.Image = "ghcr.io/han0110/zisk/zisk"
	}
	if cfg.ImageTag == "" {
		cfg.ImageTag = "1.0.0-alpha"
	}
	nodes := []*cluster.Node{&cfg.Coordinator}
	for i := range cfg.Workers {
		nodes = append(nodes, &cfg.Workers[i].Node)
		if cfg.Workers[i].Gpus == "" {
			cfg.Workers[i].Gpus = "all"
		}
	}
	cluster.ApplyNodeDefaults(nodes...)
	if cfg.Config.ShmSizeGB == 0 {
		cfg.Config.ShmSizeGB = 64
	}
	if cfg.Config.MPINp == 0 {
		cfg.Config.MPINp = 1
	}
}

func validate(cfg *Config) error {
	if cfg.Zkvm != "zisk" {
		return fmt.Errorf("zkvm %q is not zisk", cfg.Zkvm)
	}
	if cfg.Verbose < 0 || cfg.Verbose > 2 {
		return fmt.Errorf("verbose %d is out of range 0 to 2", cfg.Verbose)
	}
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}
	seenSSH := map[string]bool{}
	seenName := map[string]bool{}
	for _, worker := range cfg.Workers {
		if seenSSH[worker.SSH] {
			return fmt.Errorf("duplicate worker host %q, one worker entry per host", worker.SSH)
		}
		if seenName[worker.Name] {
			return fmt.Errorf("duplicate worker name %q", worker.Name)
		}
		seenSSH[worker.SSH] = true
		seenName[worker.Name] = true
		if err := cluster.ValidateColocation(cfg.Coordinator, worker.Node); err != nil {
			return err
		}
		if _, err := parseGpus(worker.Gpus); err != nil {
			return err
		}
	}

	for name, value := range map[string]int{
		"shm_size_gb":            cfg.Config.ShmSizeGB,
		"mpi_np":                 cfg.Config.MPINp,
		"mpi_numa_ppr":           cfg.Config.MPINumaPpr,
		"mpi_threads":            cfg.Config.MPIThreads,
		"max_streams":            cfg.Config.MaxStreams,
		"number_threads_witness": cfg.Config.NumberThreadsWitness,
		"max_witness_stored":     cfg.Config.MaxWitnessStored,
	} {
		if value < 0 {
			return fmt.Errorf("%s expects a non-negative integer, got %d", name, value)
		}
	}
	return nil
}
