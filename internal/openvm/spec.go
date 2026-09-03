package openvm

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/pelletier/go-toml/v2"

	"github.com/han0110/provoor/internal/cluster"
)

// The API and worker ports are fixed so the manager, the workers, and the
// callers agree without configuration. The final proofs and metrics
// directories bind one host path every deployment, so completed proofs
// survive teardown.
const (
	apiPort               = 3000
	workerPortBase        = 8001
	coordinatorContainer  = "openvm-coordinator"
	workerContainerPrefix = "openvm-worker-"
	keygenContainer       = "openvm-keygen"
	artifactsDir          = "/data/artifacts"
	metricsOutputDir      = "/data/metrics"
	rvrCacheDir           = "/var/cache/openvm-rvr"
	guestELFPath          = "/guests/guest.elf"
	managerConfig         = "/tmp/openvm-manager.toml"
	finalProofsDir        = "/var/tmp/openvm-final-proofs"
	metricsDir            = "/var/tmp/openvm-metrics"
	// workerReadyLine is what a worker logs once its socket binds, after it
	// built every loadout program. The pinned image words it.
	workerReadyLine = "Edge Worker listening on"
	// programVersion is the version every loadout entry carries. Loadout
	// membership matches on it, so client and deployment fix it at zero.
	programVersion = 0
	// The aggregation shape follows the image's own deployment defaults.
	// leafPackThreshold is rendered because the manager falls back to 48 for
	// an absent key, which is not what a deployment of this image runs.
	leafArity         = 4
	internalArity     = 3
	leafPackThreshold = 1000
	// vpmmPageSize is the page size of the workers' pinned memory GPU
	// allocator.
	vpmmPageSize = 16777216
)

// keygenScript derives one program's keyset inside a container, with its
// parameters supplied as environment.
//
//go:embed script/keygen.sh
var keygenScript string

// program is one loadout entry, named by its guest ELF's content digest, with
// the verifying key its proofs are checked against.
type program struct {
	name         string
	source       string
	elf          []byte
	verifyingKey []byte
}

// proverLimits is the [provers] table the manager and every worker carry.
// Only the workers set the segment memory.
type proverLimits struct {
	MaxAppProvers        int   `toml:"max_app_provers"`
	MaxLeafProvers       int   `toml:"max_leaf_provers"`
	MaxInternalProvers   int   `toml:"max_internal_provers"`
	DefaultSegmentMemory int64 `toml:"default_segment_memory,omitempty"`
}

// artifactsVolume keys on the OpenVM release rather than the image tag,
// because a keyset is derived under that release's VM configuration.
func artifactsVolume(zkvmVersion string) string {
	return "openvm-artifacts-" + zkvmVersion
}

// artifactsMount is the keyset volume, read-only for everything but keygen.
func artifactsMount(zkvmVersion string, readOnly bool) mount.Mount {
	return mount.Mount{
		Type:     mount.TypeVolume,
		Source:   artifactsVolume(zkvmVersion),
		Target:   artifactsDir,
		ReadOnly: readOnly,
	}
}

func workerContainer(gpu int) string {
	return workerContainerPrefix + strconv.Itoa(gpu)
}

// workerName labels one worker by its position in the configuration and the
// GPU it owns.
func workerName(index int, worker Worker) string {
	return cluster.WorkerName(index, cluster.GPU{DeviceIDs: []int{worker.deviceID()}})
}

func workerConfigPath(gpu int) string {
	return fmt.Sprintf("/tmp/openvm-worker-%d.toml", gpu)
}

// programName is the loadout name of a guest ELF, the first eight bytes of
// its SHA-256 digest in hex. A client built from another guest is refused by
// name instead of proving against the wrong keyset.
func programName(elf []byte) string {
	digest := sha256.Sum256(elf)
	return "program-" + hex.EncodeToString(digest[:8])
}

// edgePrograms renders the EDGE_PROGRAMS loadout JSON the manager and every
// worker receive.
func edgePrograms(programs []program) string {
	entries := make([]struct {
		Name    string `json:"name"`
		Version int    `json:"version"`
	}, len(programs))
	for i, entry := range programs {
		entries[i].Name = entry.name
		entries[i].Version = programVersion
	}
	data, _ := json.Marshal(entries)
	return string(data)
}

// workerRustLog quiets the CUDA memory manager at the default level, whose
// per-allocation logging floods the journal.
func workerRustLog(verbose int) string {
	if verbose == 0 {
		return "info,openvm_cuda_common::memory_manager=warn"
	}
	return cluster.RustLog(verbose)
}

func proverLimitsOf(cfg *Config, segmentMemory int64) proverLimits {
	return proverLimits{
		MaxAppProvers:        cfg.Prover.AppProvers,
		MaxLeafProvers:       cfg.Prover.LeafProvers,
		MaxInternalProvers:   cfg.Prover.InternalProvers,
		DefaultSegmentMemory: segmentMemory,
	}
}

// renderTOML marshals a configuration document under the generated-by
// header. Field order fixes the rendered table order.
func renderTOML(doc any) string {
	data, _ := toml.Marshal(doc)
	return "# Generated by provoor.\n" + string(data)
}

// managerTOML renders the edge-manager configuration.
func managerTOML(cfg *Config) string {
	var doc struct {
		Server struct {
			ListenAddr    string `toml:"listen_addr"`
			NumWorkers    int    `toml:"num_workers"`
			ArtifactsPath string `toml:"artifacts_path"`
		} `toml:"server"`
		Provers proverLimits `toml:"provers"`
		Proof   struct {
			LeafArity             int    `toml:"leaf_arity"`
			InternalArity         int    `toml:"internal_arity"`
			LeafPackThreshold     int    `toml:"leaf_pack_threshold"`
			TimeoutSecs           int    `toml:"timeout_secs"`
			PersistFinalProofsDir string `toml:"persist_final_proofs_dir"`
		} `toml:"proof"`
		Metrics struct {
			OutputDir string `toml:"output_dir"`
		} `toml:"metrics"`
		Telemetry struct {
			LogLevel string `toml:"log_level"`
		} `toml:"telemetry"`
	}
	doc.Server.ListenAddr = fmt.Sprintf("0.0.0.0:%d", apiPort)
	doc.Server.NumWorkers = len(cfg.Workers)
	doc.Server.ArtifactsPath = artifactsDir
	doc.Provers = proverLimitsOf(cfg, 0)
	doc.Proof.LeafArity = leafArity
	doc.Proof.InternalArity = internalArity
	doc.Proof.LeafPackThreshold = leafPackThreshold
	doc.Proof.TimeoutSecs = cfg.Prover.TimeoutSecs
	doc.Proof.PersistFinalProofsDir = finalProofsDir
	doc.Metrics.OutputDir = metricsOutputDir
	doc.Telemetry.LogLevel = cluster.RustLog(cfg.Verbose)
	return renderTOML(doc)
}

// managerURL is the manager address a worker registers against. A worker
// co-located with the coordinator dials loopback.
func managerURL(cfg *Config, worker Worker) string {
	host := cfg.Coordinator.IP
	if worker.SSH == cfg.Coordinator.SSH {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("http://%s:%d", host, apiPort)
}

// workerURL is the address a worker advertises for the manager to dial back,
// loopback for a co-located worker that names none.
func workerURL(worker Worker) string {
	host := worker.IP
	if host == "" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("http://%s:%d", host, workerPortBase+worker.deviceID())
}

// workerTOML renders one worker container's configuration. proverID is the
// worker's position in the configuration, which shards proving work.
func workerTOML(cfg *Config, worker Worker, proverID int) string {
	var doc struct {
		Server struct {
			ListenAddr string `toml:"listen_addr"`
		} `toml:"server"`
		Worker struct {
			ProverID   int    `toml:"prover_id"`
			NumProvers int    `toml:"num_provers"`
			WorkerURL  string `toml:"worker_url"`
			ManagerURL string `toml:"manager_url"`
		} `toml:"worker"`
		Artifacts struct {
			ArtifactsPath string `toml:"artifacts_path"`
		} `toml:"artifacts"`
		Provers   proverLimits `toml:"provers"`
		Telemetry struct {
			LogLevel string `toml:"log_level"`
		} `toml:"telemetry"`
	}
	doc.Server.ListenAddr = fmt.Sprintf("0.0.0.0:%d", workerPortBase+worker.deviceID())
	doc.Worker.ProverID = proverID
	doc.Worker.NumProvers = len(cfg.Workers)
	doc.Worker.WorkerURL = workerURL(worker)
	doc.Worker.ManagerURL = managerURL(cfg, worker)
	doc.Artifacts.ArtifactsPath = artifactsDir
	doc.Provers = proverLimitsOf(cfg, cfg.Prover.SegmentMemory)
	doc.Telemetry.LogLevel = workerRustLog(cfg.Verbose)
	return renderTOML(doc)
}

// cpusetCPUs is one worker container's even share of its host's CPUs, by
// position among the host's workers, with the last taking the remainder.
func cpusetCPUs(position, hostWorkers, hostCPUs int) string {
	share := hostCPUs / hostWorkers
	last := (position+1)*share - 1
	if position == hostWorkers-1 {
		last = hostCPUs - 1
	}
	return fmt.Sprintf("%d-%d", position*share, last)
}

func coordinatorSpec(cfg *Config, loadout string) cluster.Container {
	return cluster.Container{
		Name: coordinatorContainer,
		Config: &container.Config{
			Image: cfg.ImageRef(),
			Env: []string{
				"RUST_LOG=" + cluster.RustLog(cfg.Verbose),
				"RUST_BACKTRACE=1",
				"NO_COLOR=1",
				"EDGE_PROGRAMS=" + loadout,
			},
			Cmd: []string{"edge-manager", "--config", managerConfig},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:   "host",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyOnFailure},
			LogConfig:     cluster.Journald(coordinatorContainer),
			Mounts: []mount.Mount{
				artifactsMount(cfg.ZkvmVersion, true),
				{
					Type:        mount.TypeBind,
					Source:      finalProofsDir,
					Target:      finalProofsDir,
					BindOptions: &mount.BindOptions{CreateMountpoint: true},
				},
				{
					Type:        mount.TypeBind,
					Source:      metricsDir,
					Target:      metricsOutputDir,
					BindOptions: &mount.BindOptions{CreateMountpoint: true},
				},
			},
		},
		Files: map[string][]byte{managerConfig: []byte(managerTOML(cfg))},
	}
}

func workerSpec(cfg *Config, worker Worker, proverID int, cpuset, loadout string) cluster.Container {
	return cluster.Container{
		Name: workerContainer(worker.deviceID()),
		Config: &container.Config{
			Image: cfg.ImageRef(),
			Env: []string{
				"RUST_LOG=" + workerRustLog(cfg.Verbose),
				"RUST_BACKTRACE=1",
				"NO_COLOR=1",
				"VPMM_PAGE_SIZE=" + strconv.Itoa(vpmmPageSize),
				"VPMM_PAGES=0",
				"EDGE_PROGRAMS=" + loadout,
				"OPENVM_RVR_NATIVE_CACHE_DIR=" + rvrCacheDir,
			},
			Cmd: []string{"edge-worker", "--config", workerConfigPath(worker.deviceID())},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:   "host",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyOnFailure},
			ShmSize:       int64(cfg.Prover.ShmSizeGB) << 30,
			CapAdd:        []string{"SYS_NICE"},
			LogConfig:     cluster.Journald(workerContainer(worker.deviceID())),
			Mounts: []mount.Mount{
				artifactsMount(cfg.ZkvmVersion, true),
				// The rvr cache keys on the release too, since a native
				// compile targets its runtime.
				{
					Type:   mount.TypeVolume,
					Source: "openvm-rvr-cache-" + cfg.ZkvmVersion,
					Target: rvrCacheDir,
				},
			},
			Resources: container.Resources{
				CpusetCpus: cpuset,
				Ulimits:    cluster.MemlockUnlimited(),
				DeviceRequests: []container.DeviceRequest{{
					DeviceIDs:    []string{strconv.Itoa(worker.deviceID())},
					Capabilities: [][]string{{"gpu"}},
				}},
			},
		},
		Files: map[string][]byte{workerConfigPath(worker.deviceID()): []byte(workerTOML(cfg, worker, proverID))},
	}
}

// keygenSpec derives one program's keyset with the guest ELF injected, so no
// file touches the remote host filesystem.
func keygenSpec(cfg *Config, entry program) cluster.Container {
	return cluster.Container{
		Name: keygenContainer,
		Config: &container.Config{
			Image: cfg.ImageRef(),
			Env: []string{
				"ARTIFACTS_DIR=" + artifactsDir,
				"PROGRAM_NAME=" + entry.name,
				"PROGRAM_VERSION=" + strconv.Itoa(programVersion),
				"GUEST_ELF=" + guestELFPath,
			},
			Cmd: []string{"bash", "-c", keygenScript},
		},
		HostConfig: &container.HostConfig{
			ShmSize: int64(cfg.Prover.ShmSizeGB) << 30,
			Mounts:  []mount.Mount{artifactsMount(cfg.ZkvmVersion, false)},
			Resources: container.Resources{
				Ulimits: cluster.MemlockUnlimited(),
				DeviceRequests: []container.DeviceRequest{
					{Count: -1, Capabilities: [][]string{{"gpu"}}},
				},
			},
		},
		Files: map[string][]byte{guestELFPath: entry.elf},
	}
}

// artifactsShellSpec runs one shell snippet over the artifacts volume.
func artifactsShellSpec(cfg *Config, name, script string, readOnly bool) cluster.Container {
	return cluster.Container{
		Name:       name,
		Config:     &container.Config{Image: cfg.ImageRef(), Cmd: []string{"bash", "-c", script}},
		HostConfig: &container.HostConfig{Mounts: []mount.Mount{artifactsMount(cfg.ZkvmVersion, readOnly)}},
	}
}
