//go:build dockergpu

// This file exercises the real sidecar against a real DCGM host engine. It
// needs a local Docker daemon, an NVIDIA GPU, and a running
// nvidia-dcgm.service or an equivalent host engine on 127.0.0.1:5555. It is
// opt-in.
//
//	go test ./pkg/telemetry/ -tags dockergpu -v -timeout 10m
package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/han0110/provoor/pkg/cluster"
)

// scrape reads the sidecar and returns every series keyed by metric name,
// dropping the labels, which is enough for a single-GPU host.
func scrape(t *testing.T) map[string]float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", ExporterPort))
	if err != nil {
		t.Fatalf("scraping the sidecar: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, found := strings.Cut(line, "{")
		if !found {
			continue
		}
		_, value, found := strings.Cut(rest, "} ")
		if !found {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.Fields(value)[0], 64)
		if err != nil {
			continue
		}
		out[name] = parsed
	}
	return out
}

// startSidecar runs the sidecar the way provoor up does and waits for the
// first samples to land.
func startSidecar(t *testing.T, node string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cli, err := cluster.Dial("")
	if err != nil {
		t.Fatalf("dialing the local docker daemon: %v", err)
	}
	t.Cleanup(func() {
		_ = Stop(context.Background(), cli, cluster.SidecarDCGM, node)
		_ = cli.Close()
	})
	if err := Start(ctx, cli, cluster.SidecarDCGM, node, 100*time.Millisecond); err != nil {
		t.Fatalf("starting the sidecar: %v", err)
	}
	// The profiling watch warms up over the first few seconds.
	time.Sleep(15 * time.Second)
}

// TestSidecarPublishesEveryField is the end-to-end check. Every field in the
// embedded list must reach the scrape, or the window arithmetic silently
// loses a column.
func TestSidecarPublishesEveryField(t *testing.T) {
	startSidecar(t, "fieldstest")

	series := scrape(t)
	for _, line := range strings.Split(strings.TrimSpace(string(Fields)), "\n") {
		name := strings.TrimSpace(strings.Split(line, ",")[0])
		if _, ok := series[name]; !ok {
			t.Errorf("field %s is absent from the scrape", name)
		}
	}
}

// TestSidecarPublishesNoHostname guards the publish path. The results scanner
// rejects a real hostname, and the sidecar runs with host networking, so the
// default labelling would carry one.
func TestSidecarPublishesNoHostname(t *testing.T) {
	startSidecar(t, "hostnametest")

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", ExporterPort))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "hostname=") {
		t.Error("the scrape carries a hostname label")
	}
}

// TestBracketingAWindowYieldsSaneRatios covers the arithmetic a consumer
// performs. Subtracting a cumulative counter at two instants must give a
// fraction for the cycle counters, and the power gauge must read a real draw.
func TestBracketingAWindowYieldsSaneRatios(t *testing.T) {
	startSidecar(t, "brackettest")

	before := scrape(t)
	time.Sleep(10 * time.Second)
	after := scrape(t)

	elapsed := after["DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL"] - before["DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL"]
	if elapsed <= 0 {
		t.Fatal("elapsed cycles did not advance across the window")
	}
	active := after["DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL"] - before["DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL"]
	power := after["DCGM_FI_DEV_POWER_USAGE"]

	t.Logf("window smActive=%.4f, latest power=%.1fW, fbUsed=%.0fMiB",
		active/elapsed, power, after["DCGM_FI_DEV_FB_USED"])

	if ratio := active / elapsed; ratio < 0 || ratio > 1.01 {
		t.Errorf("sm active ratio is %v, want a fraction", ratio)
	}
	if power <= 0 {
		t.Errorf("power reads %v, want a positive draw", power)
	}
	if total := after["DCGM_FI_DEV_FB_TOTAL"]; total <= 0 {
		t.Errorf("frame buffer capacity reads %v, want the card size", total)
	}
}
