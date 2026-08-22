package zisk

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"

	"github.com/han0110/provoor/pkg/cluster"
)

func testConfig() *Config {
	return &Config{
		Zkvm:        "zisk",
		ZkvmVersion: "1.1.0-alpha",
		Image:       "ghcr.io/han0110/provoor/zisk",
		ImageTag:    "1.0.0-alpha",
		Guests:      []cluster.Guest{{ELF: "/guests/a.elf", VK: "/guests/a.vk"}},
		Coordinator: cluster.Node{
			SSH: "user@203.0.113.1",
			IP:  "10.0.0.1",
		},
		Workers: []Worker{
			{Node: cluster.Node{SSH: "user@203.0.113.1"}, GPU: cluster.GPU{Count: 4}},
			{Node: cluster.Node{SSH: "user@203.0.113.2"}, GPU: cluster.GPU{DeviceIDs: []int{0, 1}}},
		},
		Config: WorkerConfig{ShmSizeGB: 64, MPINp: 1, CPUMops: true},
	}
}

func TestWorkerArgs(t *testing.T) {
	cfg := testConfig()
	cfg.Verbose = 1
	cfg.Config.MPINp = 2
	cfg.Config.MaxWitnessStored = 4
	cfg.Config.MinimalMemory = true

	got := workerArgs(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 2)
	want := []string{
		"--report-bindings",
		"--allow-run-as-root",
		"-np", "2",
		"-map-by", "ppr:1:numa",
		"--bind-to", "numa",
		"--rank-by", "slot",
		"-x", "RUST_LOG=debug",
		"-x", "NO_COLOR=1",
		"-x", "ZISK_HOME=/root/.zisk",
		"-x", "ZISK_WORKER_HEALTH_PORT=9101",
		"-x", "RUST_MIN_STACK=67108864",
		"zisk-worker-gpu",
		"--coordinator-url", "http://10.0.0.1:50051",
		"--worker-id", "worker_1-gpu_0_1",
		"--compute-capacity", "20",
		"--gpu",
		"--cpu-mops",
		"--proving-key", "/root/.zisk/provingKey",
		"--max-witness-stored", "4",
		"--minimal-memory",
		"-v",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("args =\n%v\nwant\n%v", got, want)
	}
}

func TestWorkerArgsCPUMops(t *testing.T) {
	cfg := testConfig()
	if !slices.Contains(workerArgs(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 1), "--cpu-mops") {
		t.Error("cpu_mops on should pass --cpu-mops")
	}
	cfg.Config.CPUMops = false
	args := workerArgs(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 1)
	if slices.Contains(args, "--cpu-mops") {
		t.Errorf("cpu_mops off should leave the GPU planner in place, args = %v", args)
	}
	// The rest of the invocation is untouched, so the flag is the only
	// difference between the two planners.
	if !containsPair(args, "--proving-key", provingKeyDir) || !slices.Contains(args, "--gpu") {
		t.Errorf("dropping --cpu-mops must not disturb its neighbours, args = %v", args)
	}
}

func TestWorkerArgsColocatedDialsLoopback(t *testing.T) {
	cfg := testConfig()
	args := workerArgs(cfg, cfg.Workers[0], cluster.WorkerName(0, cfg.Workers[0].GPU), 1)
	if !containsPair(args, "--coordinator-url", "http://127.0.0.1:50051") {
		t.Errorf("co-located worker should dial loopback, args = %v", args)
	}
}

func TestWorkerArgsExplicitConfigSkipsDerivation(t *testing.T) {
	cfg := testConfig()
	cfg.Config.MPINp = 4
	cfg.Config.MPINumaPpr = 2
	cfg.Config.MPIThreads = 16
	args := workerArgs(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 0)
	if !containsPair(args, "-map-by", "ppr:2:numa") {
		t.Errorf("explicit mpi_numa_ppr not honored, args = %v", args)
	}
	if !containsPair(args, "-x", "RAYON_NUM_THREADS=16") {
		t.Errorf("explicit mpi_threads not honored, args = %v", args)
	}
}

func TestWorkerArgsDerivationFloorsAtOne(t *testing.T) {
	cfg := testConfig()
	args := workerArgs(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 4)
	if !containsPair(args, "-map-by", "ppr:1:numa") {
		t.Errorf("ppr should floor at 1, args = %v", args)
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "RAYON_NUM_THREADS=") {
			t.Errorf("unset mpi_threads should leave proofman's default, args = %v", args)
		}
	}
}

func containsPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func TestCoordinatorSpec(t *testing.T) {
	containerCfg, hostCfg := coordinatorSpec(testConfig())
	if containerCfg.Image != "ghcr.io/han0110/provoor/zisk:1.0.0-alpha" {
		t.Errorf("Image = %q", containerCfg.Image)
	}
	wantCmd := []string{
		"zisk-coordinator",
		"--api-port", "7000",
		"--cluster-port", "50051",
		"--config", "/tmp/zisk-coordinator.toml",
	}
	if !reflect.DeepEqual([]string(containerCfg.Cmd), wantCmd) {
		t.Errorf("Cmd = %v", containerCfg.Cmd)
	}
	if !reflect.DeepEqual(containerCfg.Env, []string{"RUST_LOG=info"}) {
		t.Errorf("Env = %v", containerCfg.Env)
	}
	if hostCfg.RestartPolicy.Name != container.RestartPolicyOnFailure {
		t.Errorf("RestartPolicy = %v", hostCfg.RestartPolicy)
	}
	if hostCfg.LogConfig.Type != "journald" || hostCfg.LogConfig.Config["tag"] != "zisk-coordinator" {
		t.Errorf("LogConfig = %+v", hostCfg.LogConfig)
	}
	for _, port := range []string{"7000/tcp", "50051/tcp"} {
		bindings := hostCfg.PortBindings[nat.Port(port)]
		if len(bindings) != 1 || bindings[0].HostPort != port[:len(port)-4] {
			t.Errorf("PortBindings[%s] = %v", port, bindings)
		}
	}
	if len(hostCfg.Mounts) != 1 || hostCfg.Mounts[0].Source != "zisk-cache-1.1.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/cache" || hostCfg.Mounts[0].ReadOnly {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
}

// TestCoordinatorEndpoint pins that the client API is addressed as the
// coordinator host sees it, never as the data network does, since a remote
// deployment reaches it through the coordinator's SSH destination.
func TestCoordinatorEndpoint(t *testing.T) {
	if got := coordinatorEndpoint(); got != "http://127.0.0.1:7000" {
		t.Errorf("endpoint = %q", got)
	}
}

func TestWorkerSpec(t *testing.T) {
	cfg := testConfig()
	containerCfg, hostCfg := workerSpec(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 1)
	if !reflect.DeepEqual([]string(containerCfg.Entrypoint), []string{"mpirun"}) {
		t.Errorf("Entrypoint = %v", containerCfg.Entrypoint)
	}
	if hostCfg.NetworkMode != "host" {
		t.Errorf("NetworkMode = %v", hostCfg.NetworkMode)
	}
	if hostCfg.ShmSize != 64<<30 {
		t.Errorf("ShmSize = %d", hostCfg.ShmSize)
	}
	if hostCfg.RestartPolicy.Name != container.RestartPolicyOnFailure {
		t.Errorf("RestartPolicy = %v", hostCfg.RestartPolicy)
	}
	if hostCfg.LogConfig.Config["tag"] != "zisk-worker" {
		t.Errorf("LogConfig = %+v", hostCfg.LogConfig)
	}
	if len(hostCfg.Mounts) != 2 ||
		hostCfg.Mounts[0].Source != "zisk-proving-key-1.1.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" ||
		hostCfg.Mounts[0].ReadOnly ||
		hostCfg.Mounts[1].Source != "zisk-cache-1.1.0-alpha" ||
		hostCfg.Mounts[1].Target != "/root/.zisk/cache" ||
		hostCfg.Mounts[1].ReadOnly {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	if len(hostCfg.Ulimits) != 1 || hostCfg.Ulimits[0].Name != "memlock" ||
		hostCfg.Ulimits[0].Soft != -1 || hostCfg.Ulimits[0].Hard != -1 {
		t.Errorf("Ulimits = %+v", hostCfg.Ulimits)
	}
	if len(hostCfg.DeviceRequests) != 1 ||
		!reflect.DeepEqual(hostCfg.DeviceRequests[0].DeviceIDs, []string{"0", "1"}) {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}

// TestWorkerHealthProbe drives the real probe against a stub endpoint with a
// recording pkill on PATH, pinning that only the worker's own unhealthy verdict
// kills the ranks. An endpoint that is not listening yet must report unhealthy
// and leave the process alone, otherwise a worker would be killed every probe
// while it is still starting or while the coordinator is unreachable.
func TestWorkerHealthProbe(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	closed := false
	defer func() {
		if !closed {
			server.Close()
		}
	}()

	port, err := strconv.Atoi(server.URL[strings.LastIndex(server.URL, ":")+1:])
	if err != nil {
		t.Fatalf("port from %s: %v", server.URL, err)
	}

	binDir := t.TempDir()
	marker := filepath.Join(binDir, "killed")
	if err := os.WriteFile(filepath.Join(binDir, "pkill"),
		[]byte("#!/bin/sh\ntouch "+marker+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	run := func() (healthy, killed bool) {
		os.Remove(marker)
		cmd := exec.Command("sh", "-c", workerHealthProbe(port))
		cmd.Env = append(os.Environ(), "PATH="+binDir+":"+os.Getenv("PATH"))
		runErr := cmd.Run()
		_, statErr := os.Stat(marker)
		return runErr == nil, statErr == nil
	}

	if healthy, killed := run(); !healthy || killed {
		t.Errorf("serving 200: healthy = %v, killed = %v, want true and false", healthy, killed)
	}

	status.Store(http.StatusServiceUnavailable)
	if healthy, killed := run(); healthy || !killed {
		t.Errorf("serving 503: healthy = %v, killed = %v, want false and true", healthy, killed)
	}

	server.Close()
	closed = true
	if healthy, killed := run(); healthy || killed {
		t.Errorf("not listening: healthy = %v, killed = %v, want false and false", healthy, killed)
	}
}

func TestCoordinatorTOML(t *testing.T) {
	want := `# Generated by provoor. Holds a job until the full cluster is Ready.
[backend]
mode = "coordinator"

[coordinator]
# Self-reference so the embedded engine re-reads this file and applies the floor.
config_file = "/tmp/zisk-coordinator.toml"
# 10 compute units per gpu over 6 gpus.
min_compute_units = 60
# Aggregation budget, covering the straggler tail of proof generation because
# its clock starts at the first worker to finish.
phase3_timeout_seconds = 600
`
	if got := coordinatorTOML(testConfig().Workers); got != want {
		t.Errorf("toml =\n%s\nwant\n%s", got, want)
	}
}

func TestSetupSpec(t *testing.T) {
	containerCfg, hostCfg := setupSpec(testConfig(), cluster.GPU{Count: 4})
	if !reflect.DeepEqual([]string(containerCfg.Cmd), []string{"bash", "-c", setupScript}) {
		t.Errorf("Cmd = %v", containerCfg.Cmd)
	}
	wantEnv := []string{
		"KEY_URL=https://storage.googleapis.com/zisk-setup/zisk-provingkey-1.1.0-alpha.tar.gz",
		"PROVING_KEY_DIR=/root/.zisk/provingKey",
	}
	if !reflect.DeepEqual(containerCfg.Env, wantEnv) {
		t.Errorf("Env = %v", containerCfg.Env)
	}
	for _, required := range []string{"set -euo pipefail", "${KEY_URL}", "${PROVING_KEY_DIR}", "check-setup"} {
		if !strings.Contains(setupScript, required) {
			t.Errorf("setup script lacks %q", required)
		}
	}
	if hostCfg.ShmSize != 16<<30 {
		t.Errorf("ShmSize = %d", hostCfg.ShmSize)
	}
	if len(hostCfg.Mounts) != 1 || hostCfg.Mounts[0].Source != "zisk-proving-key-1.1.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	// The proving-key job builds const-trees on the same GPUs the worker
	// proves on, so it claims the host's selection rather than every device.
	if len(hostCfg.DeviceRequests) != 1 || hostCfg.DeviceRequests[0].Count != 4 {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}
