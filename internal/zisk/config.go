// Package zisk deploys a ZisK proving cluster and proves against it. A
// coordinator container serves the client API and one GPU worker container
// per host registers against it.
package zisk

import (
	"fmt"

	"github.com/han0110/provoor/internal/cluster"
)

// Config describes one ZisK cluster deployment.
type Config struct {
	cluster.Config `yaml:",inline"`
	Workers        []Worker     `yaml:"workers"`
	Prover         ProverConfig `yaml:"config"`
}

// Worker is one GPU host, whose single worker container spans the GPUs it
// exposes. The compute units the worker advertises derive from the selection,
// so the coordinator's readiness floor tracks the GPUs deployed.
type Worker struct {
	cluster.Node `yaml:",inline"`
	GPU          cluster.GPU `yaml:"gpu"`
}

// ProverConfig applies to every worker. Every field but ShmSizeGB maps onto
// one mpirun or zisk-worker-gpu flag.
type ProverConfig struct {
	// ShmSizeGB sizes /dev/shm for the ASM emulator's shared-memory trace
	// segments.
	ShmSizeGB int `yaml:"shm_size_gb"`
	// MPINp is the MPI rank count per worker.
	MPINp int `yaml:"mpi_np"`
	// MPINumaPpr is the rank count per NUMA node, by default MPINp divided by
	// the host's NUMA node count observed at deploy time, minimum 1.
	MPINumaPpr int `yaml:"mpi_numa_ppr"`
	// MPIThreads is RAYON_NUM_THREADS per rank.
	MPIThreads int `yaml:"mpi_threads"`
	// MaxStreams caps GPU streams.
	MaxStreams int `yaml:"max_streams"`
	// NumberThreadsWitness sets witness computation threads.
	NumberThreadsWitness int `yaml:"number_threads_witness"`
	// MaxWitnessStored sizes the recursive witness buffer pools. A pool of 4
	// frees roughly 10 GB of heap at no measured proving-time cost.
	MaxWitnessStored int `yaml:"max_witness_stored"`
	// MinimalMemory builds collectors per instance instead of batched, which
	// frees roughly 11 GB of heap for step-heavy blocks.
	MinimalMemory bool `yaml:"minimal_memory"`
	// CPUMops plans memory ops on the CPU. The GPU planner is roughly 6%
	// faster per proof and holds far less host memory. It caps a proof at
	// 1024 Main segments and aborts the worker above that.
	CPUMops bool `yaml:"cpu_mops"`
}

// Load reads, defaults, and validates a ZisK cluster configuration file.
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

func (cfg *Config) setDefaults() {
	cfg.Config.SetDefaults()
	if cfg.Prover.ShmSizeGB == 0 {
		cfg.Prover.ShmSizeGB = 64
	}
	if cfg.Prover.MPINp == 0 {
		cfg.Prover.MPINp = 1
	}
}

func (cfg *Config) validate() error {
	if err := cfg.Config.Validate("zisk"); err != nil {
		return err
	}
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}
	if err := cfg.Telemetry.Validate(cfg.destinations()); err != nil {
		return err
	}
	seenSSH := map[string]bool{}
	for i, worker := range cfg.Workers {
		if seenSSH[worker.SSH] {
			return fmt.Errorf("duplicate worker host %q, one worker entry per host", worker.SSH)
		}
		seenSSH[worker.SSH] = true
		if err := cluster.ValidateColocation(cfg.Coordinator, worker.Node); err != nil {
			return err
		}
		if err := worker.GPU.Validate(); err != nil {
			return fmt.Errorf("worker %d: %w", i, err)
		}
	}
	for name, value := range map[string]int{
		"shm_size_gb":            cfg.Prover.ShmSizeGB,
		"mpi_np":                 cfg.Prover.MPINp,
		"mpi_numa_ppr":           cfg.Prover.MPINumaPpr,
		"mpi_threads":            cfg.Prover.MPIThreads,
		"max_streams":            cfg.Prover.MaxStreams,
		"number_threads_witness": cfg.Prover.NumberThreadsWitness,
		"max_witness_stored":     cfg.Prover.MaxWitnessStored,
	} {
		if value < 0 {
			return fmt.Errorf("%s expects a non-negative integer, got %d", name, value)
		}
	}
	return nil
}

// destinations lists every host the deployment dials, the coordinator first.
func (cfg *Config) destinations() []string {
	destinations := []string{cfg.Coordinator.SSH}
	for _, worker := range cfg.Workers {
		destinations = append(destinations, worker.SSH)
	}
	return destinations
}
