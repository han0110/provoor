//go:build dockergpu

// These tests run the real sidecars against the local Docker daemon. The DCGM
// ones need an NVIDIA GPU and a host engine on 127.0.0.1:5555.
//
//	go test ./internal/cluster/ -tags dockergpu -v -timeout 10m
package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func localDaemon(t *testing.T) *client.Client {
	t.Helper()
	cli, err := dial("")
	if err != nil {
		t.Fatalf("dialing the local docker daemon: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// runSidecar starts one sidecar the way provoor up does and removes it after
// the test.
func runSidecar(t *testing.T, cli *client.Client, kind, node string, interval time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	t.Cleanup(func() { _ = stopSidecar(context.Background(), cli, kind, node) })
	if err := startSidecar(ctx, cli, kind, node, interval); err != nil {
		t.Fatalf("starting the %s sidecar: %v", kind, err)
	}
}

func scrapeText(t *testing.T, port int) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("scraping the sidecar: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// scrapeDCGM returns every series keyed by metric name, dropping the labels,
// which is enough for a single-GPU host.
func scrapeDCGM(t *testing.T) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, line := range strings.Split(scrapeText(t, dcgmPort), "\n") {
		name, rest, found := strings.Cut(line, "{")
		if !found || strings.HasPrefix(line, "#") {
			continue
		}
		_, value, found := strings.Cut(rest, "} ")
		if !found {
			continue
		}
		if parsed, err := strconv.ParseFloat(strings.Fields(value)[0], 64); err == nil {
			out[name] = parsed
		}
	}
	return out
}

func TestDCGMSidecarPublishesEveryFieldAndNoHostname(t *testing.T) {
	runSidecar(t, localDaemon(t), sidecarDCGM, "fieldstest", 100*time.Millisecond)
	// The profiling watch warms up over the first seconds.
	time.Sleep(15 * time.Second)

	text := scrapeText(t, dcgmPort)
	if strings.Contains(text, "hostname=") {
		t.Error("the scrape carries a hostname label")
	}
	series := scrapeDCGM(t)
	for _, line := range strings.Split(strings.TrimSpace(string(dcgmFields)), "\n") {
		name := strings.TrimSpace(strings.Split(line, ",")[0])
		if _, ok := series[name]; !ok {
			t.Errorf("field %s is absent from the scrape", name)
		}
	}
}

// TestDCGMWindowYieldsSaneRatios checks that a cumulative counter subtracted
// at two instants gives a fraction and the power gauge reads a real draw.
func TestDCGMWindowYieldsSaneRatios(t *testing.T) {
	runSidecar(t, localDaemon(t), sidecarDCGM, "brackettest", 100*time.Millisecond)
	time.Sleep(15 * time.Second)

	before := scrapeDCGM(t)
	time.Sleep(10 * time.Second)
	after := scrapeDCGM(t)
	elapsed := after["DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL"] - before["DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL"]
	if elapsed <= 0 {
		t.Fatal("elapsed cycles did not advance across the window")
	}
	active := after["DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL"] - before["DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL"]
	if ratio := active / elapsed; ratio < 0 || ratio > 1.01 {
		t.Errorf("sm active ratio is %v, want a fraction", ratio)
	}
	if power := after["DCGM_FI_DEV_POWER_USAGE"]; power <= 0 {
		t.Errorf("power reads %v, want a positive draw", power)
	}
	if total := after["DCGM_FI_DEV_FB_TOTAL"]; total <= 0 {
		t.Errorf("frame buffer capacity reads %v, want the card size", total)
	}
}

func TestStartSidecarReplacesALeftover(t *testing.T) {
	cli := localDaemon(t)
	const node = "replacetest"
	runSidecar(t, cli, sidecarDCGM, node, time.Second)
	first, err := cli.ContainerInspect(t.Context(), sidecarName(sidecarDCGM, node))
	if err != nil {
		t.Fatal(err)
	}
	runSidecar(t, cli, sidecarDCGM, node, time.Second)
	second, err := cli.ContainerInspect(t.Context(), sidecarName(sidecarDCGM, node))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Error("the second start reused the old container, want it replaced")
	}
	if n := len(second.HostConfig.Resources.DeviceRequests); n != 0 {
		t.Errorf("the sidecar holds %d device requests, want none", n)
	}
	if second.HostConfig.NetworkMode != container.NetworkMode("host") {
		t.Errorf("network mode is %q, want host", second.HostConfig.NetworkMode)
	}
}

func TestStopSidecarIsIdempotent(t *testing.T) {
	if err := stopSidecar(t.Context(), localDaemon(t), sidecarDCGM, "neverstarted"); err != nil {
		t.Errorf("stopping an absent sidecar returned %v, want nil", err)
	}
}

// TestNodeSidecarReportsTheMachine checks that /proc inside the container
// still describes the machine, since the sidecar mounts nothing from the host.
func TestNodeSidecarReportsTheMachine(t *testing.T) {
	runSidecar(t, localDaemon(t), sidecarNode, "nodetest", 0)
	text := scrapeText(t, nodePort)

	memTotal := regexp.MustCompile(`(?m)^node_memory_MemTotal_bytes (\S+)$`).FindStringSubmatch(text)
	if memTotal == nil {
		t.Fatal("the scrape carries no node_memory_MemTotal_bytes")
	}
	got, err := strconv.ParseFloat(memTotal[1], 64)
	if err != nil {
		t.Fatal(err)
	}
	if want := hostMemTotalBytes(t); got != want {
		t.Errorf("memory total reads %v, want the host's %v", got, want)
	}
	cpus := regexp.MustCompile(`(?m)^node_cpu_seconds_total\{cpu="(\d+)",mode="idle"\}`).FindAllStringSubmatch(text, -1)
	if len(cpus) != runtime.NumCPU() {
		t.Errorf("the scrape names %d processors, want the host's %d", len(cpus), runtime.NumCPU())
	}
	for _, family := range []string{"node_filesystem_", "node_disk_", "node_network_"} {
		if strings.Contains(text, family) {
			t.Errorf("the scrape carries %s series, want only the processor and memory collectors", family)
		}
	}
	if strings.Contains(text, "hostname=") {
		t.Error("the scrape carries a hostname label")
	}
}

func hostMemTotalBytes(t *testing.T) float64 {
	t.Helper()
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if fields := strings.Fields(line); len(fields) == 3 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				t.Fatal(err)
			}
			return kb * 1024
		}
	}
	t.Fatal("no MemTotal in /proc/meminfo")
	return 0
}
