//go:build dockergpu

// Sidecar lifecycle against a real Docker daemon. The metric content is
// covered by exporter_docker_test.go, so this file stays on start and stop
// behaviour.
//
//	go test ./pkg/telemetry/ -tags dockergpu -v -timeout 10m
package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"

	"github.com/han0110/provoor/pkg/cluster"
)

func TestStartReplacesALeftoverSidecar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cli, err := cluster.Dial("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	const node = "replacetest"
	t.Cleanup(func() { _ = Stop(context.Background(), cli, node) })

	if err := Start(ctx, cli, node, time.Second); err != nil {
		t.Fatalf("first start: %v", err)
	}
	first, err := cli.ContainerInspect(ctx, SidecarName(node))
	if err != nil {
		t.Fatal(err)
	}

	// A sidecar left from an earlier deployment must be replaced, so the
	// sample stream always matches the field set this build requests.
	if err := Start(ctx, cli, node, time.Second); err != nil {
		t.Fatalf("second start: %v", err)
	}
	second, err := cli.ContainerInspect(ctx, SidecarName(node))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Error("the second start reused the old container, want it replaced")
	}
}

func TestStopIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cli, err := cluster.Dial("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// Removing a sidecar that never existed must not fail, so a deployment
	// tears down the same way whether or not telemetry ran.
	if err := Stop(ctx, cli, "neverstarted"); err != nil {
		t.Errorf("stopping an absent sidecar returned %v, want nil", err)
	}
}

func TestSidecarRunsWithoutDeviceAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cli, err := cluster.Dial("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	const node = "nodevicetest"
	t.Cleanup(func() { _ = Stop(context.Background(), cli, node) })

	if err := Start(ctx, cli, node, time.Second); err != nil {
		t.Fatal(err)
	}
	info, err := cli.ContainerInspect(ctx, SidecarName(node))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(info.HostConfig.Resources.DeviceRequests); n != 0 {
		t.Errorf("the sidecar holds %d device requests, want none", n)
	}
	if n := len(info.HostConfig.CapAdd); n != 0 {
		t.Errorf("the sidecar holds capabilities %v, want none", info.HostConfig.CapAdd)
	}
	if info.HostConfig.NetworkMode != container.NetworkMode("host") {
		t.Errorf("network mode is %q, want host", info.HostConfig.NetworkMode)
	}
}
