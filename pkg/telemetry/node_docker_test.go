//go:build dockergpu

package telemetry

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

	"github.com/han0110/provoor/pkg/cluster"
)

// hostMemTotalBytes reads the machine's own memory total, the figure the
// sidecar must report from inside its container.
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

// TestNodeSidecarReportsTheMachine covers the node sidecar against the local
// daemon. It needs no GPU. The sidecar mounts nothing from the host, so the
// test proves that /proc inside the container still describes the machine by
// matching the memory total and the processor count against the host.
func TestNodeSidecarReportsTheMachine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cli, err := cluster.Dial("")
	if err != nil {
		t.Fatal(err)
	}
	const node = "nodetest"
	t.Cleanup(func() {
		_ = Stop(context.Background(), cli, cluster.SidecarNode, node)
		_ = cli.Close()
	})
	if err := Start(ctx, cli, cluster.SidecarNode, node, 0); err != nil {
		t.Fatalf("starting the node sidecar: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", NodePort))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)

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
