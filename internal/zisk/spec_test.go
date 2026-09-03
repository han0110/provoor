package zisk

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"

	"github.com/han0110/provoor/internal/cluster"
)

func testConfig() *Config {
	return &Config{
		Config: cluster.Config{
			Zkvm:        "zisk",
			ZkvmVersion: "1.2.0-alpha",
			Image:       "ghcr.io/han0110/provoor/zisk",
			ImageTag:    "1.0.0-alpha",
			Guests:      []cluster.Guest{{ELF: "/guests/a.elf", VK: "/guests/a.vk"}},
			Coordinator: cluster.Node{SSH: "user@203.0.113.1", IP: "10.0.0.1"},
		},
		Workers: []Worker{
			{Node: cluster.Node{SSH: "user@203.0.113.1"}, GPU: cluster.GPU{Count: 4}},
			{Node: cluster.Node{SSH: "user@203.0.113.2"}, GPU: cluster.GPU{DeviceIDs: []int{0, 1}}},
		},
		Prover: ProverConfig{ShmSizeGB: 64, MPINp: 1, CPUMops: true},
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

func TestWorkerArgs(t *testing.T) {
	loaded, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		configure func(*Config)
		worker    int
		numaNodes int
		want      []string
		pairs     [][2]string
		// absent lists argument prefixes the invocation must not carry.
		absent []string
	}{
		{
			name: "full",
			configure: func(cfg *Config) {
				cfg.Verbose = 1
				cfg.Prover.MPINp = 2
				cfg.Prover.MaxWitnessStored = 4
				cfg.Prover.MinimalMemory = true
			},
			worker:    1,
			numaNodes: 2,
			want: []string{
				"--report-bindings",
				"--allow-run-as-root",
				"-np", "2",
				"-map-by", "ppr:1:numa",
				"--bind-to", "numa",
				"--rank-by", "slot",
				"-x", "RUST_LOG=debug",
				"-x", "NO_COLOR=1",
				"-x", "ZISK_HOME=/root/.zisk",
				"-x", "ZISK_WORKER_HEALTH_PORT=7001",
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
			},
		},
		{
			name:      "cpu mops off leaves the gpu planner",
			configure: func(cfg *Config) { cfg.Prover.CPUMops = false },
			worker:    1,
			numaNodes: 1,
			pairs:     [][2]string{{"--gpu", "--proving-key"}, {"--proving-key", provingKeyDir}},
			absent:    []string{"--cpu-mops"},
		},
		{
			name:      "colocated dials loopback",
			worker:    0,
			numaNodes: 1,
			pairs:     [][2]string{{"--coordinator-url", "http://127.0.0.1:50051"}},
		},
		{
			name: "explicit config skips derivation",
			configure: func(cfg *Config) {
				cfg.Prover.MPINp = 4
				cfg.Prover.MPINumaPpr = 2
				cfg.Prover.MPIThreads = 16
			},
			worker:    1,
			numaNodes: 0,
			pairs:     [][2]string{{"-map-by", "ppr:2:numa"}, {"-x", "RAYON_NUM_THREADS=16"}},
		},
		{
			name:      "derivation floors at one",
			worker:    1,
			numaNodes: 4,
			pairs:     [][2]string{{"-map-by", "ppr:1:numa"}},
			absent:    []string{"RAYON_NUM_THREADS="},
		},
		{
			name:      "defaults only config leaves the knobs to the image",
			configure: func(cfg *Config) { *cfg = *loaded },
			worker:    0,
			numaNodes: 1,
			absent: []string{
				"--cpu-mops", "--minimal-memory", "--max-witness-stored",
				"--max-streams", "--number-threads-witness", "RAYON_NUM_THREADS=",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			if tc.configure != nil {
				tc.configure(cfg)
			}
			worker := cfg.Workers[tc.worker]
			got := workerArgs(cfg, worker, cluster.WorkerName(tc.worker, worker.GPU), tc.numaNodes)
			if tc.want != nil && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("args =\n%v\nwant\n%v", got, tc.want)
			}
			for _, pair := range tc.pairs {
				if !containsPair(got, pair[0], pair[1]) {
					t.Errorf("args = %v, want %v %v", got, pair[0], pair[1])
				}
			}
			for _, arg := range got {
				for _, prefix := range tc.absent {
					if strings.HasPrefix(arg, prefix) {
						t.Errorf("args = %v, want no %s", got, prefix)
					}
				}
			}
		})
	}
}

func TestCoordinatorSpec(t *testing.T) {
	spec := coordinatorSpec(testConfig())
	if spec.Name != "zisk-coordinator" {
		t.Errorf("Name = %q", spec.Name)
	}
	if spec.Config.Image != "ghcr.io/han0110/provoor/zisk:1.0.0-alpha" {
		t.Errorf("Image = %q", spec.Config.Image)
	}
	wantCmd := []string{
		"zisk-supervisor",
		"zisk-coordinator",
		"--api-port", "7000",
		"--cluster-port", "50051",
		"--config", "/tmp/zisk-coordinator.toml",
	}
	if !reflect.DeepEqual([]string(spec.Config.Cmd), wantCmd) {
		t.Errorf("Cmd = %v", spec.Config.Cmd)
	}
	if !reflect.DeepEqual(spec.Config.Env, []string{"RUST_LOG=info", "RESTART_PORT=7002"}) {
		t.Errorf("Env = %v", spec.Config.Env)
	}
	if spec.HostConfig.RestartPolicy.Name != container.RestartPolicyOnFailure {
		t.Errorf("RestartPolicy = %v", spec.HostConfig.RestartPolicy)
	}
	if spec.HostConfig.LogConfig.Type != "journald" || spec.HostConfig.LogConfig.Config["tag"] != "zisk-coordinator" {
		t.Errorf("LogConfig = %+v", spec.HostConfig.LogConfig)
	}
	for _, port := range []string{"7000/tcp", "50051/tcp", "7002/tcp"} {
		bindings := spec.HostConfig.PortBindings[nat.Port(port)]
		if len(bindings) != 1 || bindings[0].HostPort != port[:len(port)-4] {
			t.Errorf("PortBindings[%s] = %v", port, bindings)
		}
	}
	mounts := spec.HostConfig.Mounts
	if len(mounts) != 1 || mounts[0].Source != "zisk-cache-1.2.0-alpha" || mounts[0].Target != "/root/.zisk/cache" || mounts[0].ReadOnly {
		t.Errorf("Mounts = %+v", mounts)
	}
	if got := string(spec.Files["/tmp/zisk-coordinator.toml"]); got != coordinatorTOML(testConfig().Workers) {
		t.Errorf("Files = %q", got)
	}
}

func TestWorkerSpec(t *testing.T) {
	cfg := testConfig()
	spec := workerSpec(cfg, cfg.Workers[1], cluster.WorkerName(1, cfg.Workers[1].GPU), 1)
	if spec.Name != "zisk-worker" {
		t.Errorf("Name = %q", spec.Name)
	}
	if !reflect.DeepEqual([]string(spec.Config.Entrypoint), []string{"mpirun"}) {
		t.Errorf("Entrypoint = %v", spec.Config.Entrypoint)
	}
	hostCfg := spec.HostConfig
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
		hostCfg.Mounts[0].Source != "zisk-proving-key-1.2.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" ||
		hostCfg.Mounts[0].ReadOnly ||
		hostCfg.Mounts[1].Source != "zisk-cache-1.2.0-alpha" ||
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
// recording pkill on PATH, so only the worker's own unhealthy verdict kills
// the ranks.
func TestWorkerHealthProbe(t *testing.T) {
	var status atomic.Int32
	var body atomic.Value
	status.Store(http.StatusOK)
	body.Store("zisk-worker ok, event loop idle 0s\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte(body.Load().(string)))
	}))
	defer server.Close()

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

	status.Store(http.StatusOK)
	body.Store("# HELP python_gc_objects_collected_total\n")
	if healthy, killed := run(); healthy || killed {
		t.Errorf("another service on the port: healthy = %v, killed = %v, want false and false", healthy, killed)
	}

	server.Close()
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
	spec := setupSpec(testConfig(), cluster.GPU{Count: 4})
	if spec.Name != "zisk-proving-key-setup" {
		t.Errorf("Name = %q", spec.Name)
	}
	if !reflect.DeepEqual([]string(spec.Config.Cmd), []string{"bash", "-c", setupScript}) {
		t.Errorf("Cmd = %v", spec.Config.Cmd)
	}
	wantEnv := []string{
		"KEY_URL=https://storage.googleapis.com/zisk-setup/zisk-provingkey-1.2.0-alpha.tar.gz",
		"PROVING_KEY_DIR=/root/.zisk/provingKey",
	}
	if !reflect.DeepEqual(spec.Config.Env, wantEnv) {
		t.Errorf("Env = %v", spec.Config.Env)
	}
	for _, required := range []string{"set -euo pipefail", "${KEY_URL}", "${PROVING_KEY_DIR}", "check-setup"} {
		if !strings.Contains(setupScript, required) {
			t.Errorf("setup script lacks %q", required)
		}
	}
	hostCfg := spec.HostConfig
	if hostCfg.ShmSize != 16<<30 {
		t.Errorf("ShmSize = %d", hostCfg.ShmSize)
	}
	if len(hostCfg.Mounts) != 1 || hostCfg.Mounts[0].Source != "zisk-proving-key-1.2.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	// The job claims the host's own GPU selection rather than every device.
	if len(hostCfg.DeviceRequests) != 1 || hostCfg.DeviceRequests[0].Count != 4 {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}

func TestProgramSetupSpec(t *testing.T) {
	spec := programSetupSpec(testConfig(), cluster.GPU{Count: 4}, []byte("elf"), []byte("vk"))
	if spec.Name != "zisk-program-setup" {
		t.Errorf("Name = %q", spec.Name)
	}
	if !reflect.DeepEqual([]string(spec.Config.Cmd), []string{"bash", "-c", programSetupScript}) {
		t.Errorf("Cmd = %v", spec.Config.Cmd)
	}
	wantEnv := []string{
		"ELF_PATH=/tmp/zisk-guest.elf",
		"VK_PATH=/tmp/zisk-guest.vk",
		"PROVING_KEY_DIR=/root/.zisk/provingKey",
		"CACHE_DIR=/root/.zisk/cache",
		"RUST_LOG=info",
	}
	if !reflect.DeepEqual(spec.Config.Env, wantEnv) {
		t.Errorf("Env = %v", spec.Config.Env)
	}
	for _, required := range []string{"set -euo pipefail", "${ELF_PATH}", "${VK_PATH}", "${PROVING_KEY_DIR}", "${CACHE_DIR}", "program-setup"} {
		if !strings.Contains(programSetupScript, required) {
			t.Errorf("program setup script lacks %q", required)
		}
	}
	wantFiles := map[string][]byte{"/tmp/zisk-guest.elf": []byte("elf"), "/tmp/zisk-guest.vk": []byte("vk")}
	if !reflect.DeepEqual(spec.Files, wantFiles) {
		t.Errorf("Files = %v", spec.Files)
	}
	hostCfg := spec.HostConfig
	// The assembly lands in the cache the workers read, and the setup reads
	// the proving key from the volume the workers prove against.
	if len(hostCfg.Mounts) != 2 || hostCfg.Mounts[0].Source != "zisk-proving-key-1.2.0-alpha" ||
		hostCfg.Mounts[1].Source != "zisk-cache-1.2.0-alpha" || hostCfg.Mounts[1].Target != "/root/.zisk/cache" {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	if len(hostCfg.DeviceRequests) != 1 || hostCfg.DeviceRequests[0].Count != 4 {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}
