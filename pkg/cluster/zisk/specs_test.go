package zisk

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"

	"github.com/han0110/provoor/pkg/cluster"
)

func testConfig() *Config {
	return &Config{
		Zkvm:     "zisk",
		Image:    "ghcr.io/han0110/zisk/zisk",
		ImageTag: "1.0.0-alpha",
		Guests:   []cluster.Guest{{ELF: "/guests/a.elf", VK: "/guests/a.vk"}},
		Coordinator: cluster.Node{
			Name: "node1",
			SSH:  "user@203.0.113.1",
			IP:   "10.0.0.1",
		},
		Workers: []Worker{
			{Node: cluster.Node{Name: "node1", SSH: "user@203.0.113.1"}, Gpus: "all"},
			{Node: cluster.Node{Name: "node2", SSH: "user@203.0.113.2"}, Gpus: "all"},
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

	got := workerArgs(cfg, cfg.Workers[1], 2)
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
		"-x", "RUST_MIN_STACK=67108864",
		"zisk-worker-gpu",
		"--coordinator-url", "http://10.0.0.1:50051",
		"--worker-id", "node2",
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
	if !slices.Contains(workerArgs(cfg, cfg.Workers[1], 1), "--cpu-mops") {
		t.Error("cpu_mops on should pass --cpu-mops")
	}
	cfg.Config.CPUMops = false
	args := workerArgs(cfg, cfg.Workers[1], 1)
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
	args := workerArgs(cfg, cfg.Workers[0], 1)
	if !containsPair(args, "--coordinator-url", "http://127.0.0.1:50051") {
		t.Errorf("co-located worker should dial loopback, args = %v", args)
	}
}

func TestWorkerArgsExplicitConfigSkipsDerivation(t *testing.T) {
	cfg := testConfig()
	cfg.Config.MPINp = 4
	cfg.Config.MPINumaPpr = 2
	cfg.Config.MPIThreads = 16
	args := workerArgs(cfg, cfg.Workers[1], 0)
	if !containsPair(args, "-map-by", "ppr:2:numa") {
		t.Errorf("explicit mpi_numa_ppr not honored, args = %v", args)
	}
	if !containsPair(args, "-x", "RAYON_NUM_THREADS=16") {
		t.Errorf("explicit mpi_threads not honored, args = %v", args)
	}
}

func TestWorkerArgsDerivationFloorsAtOne(t *testing.T) {
	cfg := testConfig()
	args := workerArgs(cfg, cfg.Workers[1], 4)
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
	if containerCfg.Image != "ghcr.io/han0110/zisk/zisk:1.0.0-alpha" {
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
	if len(hostCfg.Mounts) != 1 || hostCfg.Mounts[0].Source != "zisk-cache-1.0.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/cache" || hostCfg.Mounts[0].ReadOnly {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
}

func TestCoordinatorEndpoint(t *testing.T) {
	cfg := testConfig()
	if got := coordinatorEndpoint(cfg); got != "http://10.0.0.1:7000" {
		t.Errorf("endpoint = %q", got)
	}
	cfg.Coordinator.IP = ""
	if got := coordinatorEndpoint(cfg); got != "http://node1:7000" {
		t.Errorf("endpoint without an ip = %q", got)
	}
	cfg.Coordinator.SSH = ""
	if got := coordinatorEndpoint(cfg); got != "http://127.0.0.1:7000" {
		t.Errorf("endpoint of a local coordinator = %q", got)
	}
}

func TestWorkerSpec(t *testing.T) {
	cfg := testConfig()
	containerCfg, hostCfg, err := workerSpec(cfg, cfg.Workers[1], 1)
	if err != nil {
		t.Fatal(err)
	}
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
		hostCfg.Mounts[0].Source != "zisk-proving-key-1.0.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" ||
		hostCfg.Mounts[0].ReadOnly ||
		hostCfg.Mounts[1].Source != "zisk-cache-1.0.0-alpha" ||
		hostCfg.Mounts[1].Target != "/root/.zisk/cache" ||
		hostCfg.Mounts[1].ReadOnly {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	if len(hostCfg.Ulimits) != 1 || hostCfg.Ulimits[0].Name != "memlock" ||
		hostCfg.Ulimits[0].Soft != -1 || hostCfg.Ulimits[0].Hard != -1 {
		t.Errorf("Ulimits = %+v", hostCfg.Ulimits)
	}
	if len(hostCfg.DeviceRequests) != 1 || hostCfg.DeviceRequests[0].Count != -1 {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}

func TestCoordinatorTOML(t *testing.T) {
	want := `# Generated by provoor. Holds a job until the full cluster is Ready.
[backend]
mode = "coordinator"

[coordinator]
# Self-reference so the embedded engine re-reads this file and applies the floor.
config_file = "/tmp/zisk-coordinator.toml"
# 10 compute units per worker over 2 workers.
min_compute_units = 20
`
	if got := coordinatorTOML(2); got != want {
		t.Errorf("toml =\n%s\nwant\n%s", got, want)
	}
}

func TestSetupSpec(t *testing.T) {
	containerCfg, hostCfg := setupSpec(testConfig())
	if !reflect.DeepEqual([]string(containerCfg.Cmd), []string{"bash", "-c", setupScript}) {
		t.Errorf("Cmd = %v", containerCfg.Cmd)
	}
	wantEnv := []string{
		"KEY_URL=https://storage.googleapis.com/zisk-setup/zisk-provingkey-1.0.0-alpha.tar.gz",
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
	if len(hostCfg.Mounts) != 1 || hostCfg.Mounts[0].Source != "zisk-proving-key-1.0.0-alpha" ||
		hostCfg.Mounts[0].Target != "/root/.zisk/provingKey" {
		t.Errorf("Mounts = %+v", hostCfg.Mounts)
	}
	if len(hostCfg.DeviceRequests) != 1 || hostCfg.DeviceRequests[0].Count != -1 {
		t.Errorf("DeviceRequests = %+v", hostCfg.DeviceRequests)
	}
}

func TestParseGpus(t *testing.T) {
	all, err := parseGpus("all")
	if err != nil || all.Count != -1 || len(all.DeviceIDs) != 0 {
		t.Errorf("all = %+v, err = %v", all, err)
	}
	two, err := parseGpus("2")
	if err != nil || two.Count != 2 {
		t.Errorf("2 = %+v, err = %v", two, err)
	}
	devices, err := parseGpus("device=0,1")
	if err != nil || !reflect.DeepEqual(devices.DeviceIDs, []string{"0", "1"}) {
		t.Errorf("device=0,1 = %+v, err = %v", devices, err)
	}
	for _, request := range []container.DeviceRequest{all, two, devices} {
		if !reflect.DeepEqual(request.Capabilities, [][]string{{"gpu"}}) {
			t.Errorf("Capabilities = %+v", request.Capabilities)
		}
	}
	if _, err := parseGpus("none"); err == nil {
		t.Error("expected error for invalid spec")
	}
}
