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
	// ZkvmVersion is the ZisK release the deployment proves under. It names
	// the proving key a host downloads and the volumes it caches, so an image
	// rebuilt under another tag still shares the artifacts of its release.
	ZkvmVersion string `yaml:"zkvm_version"`
	// Image and ImageTag name the cluster image, the tag defaulting to the
	// ZisK release it carries.
	Image    string `yaml:"image"`
	ImageTag string `yaml:"image_tag"`
	// Verbose raises container log levels, 0 info, 1 debug, 2 trace.
	Verbose int `yaml:"verbose"`
	// Guests lists the guest programs compiled into every worker host's
	// artifact cache before the cluster starts, each naming an ELF and the
	// verifying key its proofs are checked against. Compiling here is what
	// keeps the ROM and assembly generation out of the first setup a forwarder
	// asks for. Artifacts are addressed by ELF hash, so a serve-side ELF must
	// be byte-identical to its entry here to reach the cache.
	Guests      []cluster.Guest `yaml:"guests"`
	Coordinator cluster.Node    `yaml:"coordinator"`
	Workers     []Worker        `yaml:"workers"`
	// Telemetry configures the GPU metric sidecar that runs beside the
	// workers on every host. An absent block leaves it on.
	Telemetry cluster.Telemetry `yaml:"telemetry"`
	Config    WorkerConfig      `yaml:"config"`
}

// Worker is one GPU host, whose single worker container spans the GPUs it
// exposes.
type Worker struct {
	cluster.Node `yaml:",inline"`
	// GPU selects the host's GPUs the container proves on, a count or the
	// device ids. The compute units the worker advertises are derived from it,
	// so the coordinator's readiness floor tracks the GPUs actually deployed.
	GPU cluster.GPU `yaml:"gpu"`
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
	// CPUMops plans memory ops on the CPU, off by default as upstream has it.
	// The GPU planner is roughly 6% faster per proof, since the plan wait
	// drops from around 300 ms to around 115 ms, and it holds far less host
	// memory, but it caps a proof at 1024 Main segments and aborts the worker
	// above that rather than failing the job, which is why a deployment
	// proving blocks near that size turns it on.
	CPUMops bool `yaml:"cpu_mops"`
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

// applyDefaults fills in what the deployment itself decides. Every worker knob
// the cluster reads is left at its zero value when unset, so the worker image
// applies its own default rather than one this repository invents, and a
// configuration file spells out only where the deployment diverges.
func applyDefaults(cfg *Config) {
	if cfg.Image == "" {
		cfg.Image = "ghcr.io/han0110/provoor/zisk"
	}
	if cfg.ImageTag == "" {
		cfg.ImageTag = cfg.ZkvmVersion
	}
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
	if cfg.ZkvmVersion == "" {
		return fmt.Errorf("zkvm_version is required, the ZisK release the deployment proves under")
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
