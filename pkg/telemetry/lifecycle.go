package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"

	"github.com/han0110/provoor/pkg/cluster"
)

// settle is how long Start watches a new sidecar before calling it up. The
// exporter exits within a second when the host engine is unreachable or too
// old, so a container still running after this window started cleanly.
const settle = 5 * time.Second

// Start runs the telemetry sidecar on one node. A sidecar left from an
// earlier deployment is replaced rather than reused, so the field set always
// matches this build.
func Start(ctx context.Context, cli *client.Client, node string, interval time.Duration) error {
	name := SidecarName(node)
	// A pull failure is fatal only when the host has no copy already, which
	// keeps a node usable while its registry is unreachable.
	if pullErr := cluster.PullImage(ctx, cli, Image); pullErr != nil {
		if _, _, err := cli.ImageInspectWithRaw(ctx, Image); err != nil {
			return fmt.Errorf("pulling the telemetry image on %s: %w", node, pullErr)
		}
	}
	_ = cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})

	containerCfg, hostCfg := Spec(interval)
	hostCfg.LogConfig = cluster.Journald(name)
	created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, name)
	if err != nil {
		return fmt.Errorf("creating %s: %w", name, err)
	}
	if err := cluster.CopyFileToContainer(ctx, cli, created.ID, FieldsPath, Fields); err != nil {
		return fmt.Errorf("writing the field list into %s: %w", name, err)
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting %s: %w", name, err)
	}
	return waitRunning(ctx, cli, name)
}

// waitRunning reports a sidecar that fails at startup. The restart policy
// otherwise turns an unreachable or outdated host engine into a crash loop
// that a deployment never notices.
func waitRunning(ctx context.Context, cli *client.Client, name string) error {
	deadline := time.Now().Add(settle)
	for {
		info, err := cli.ContainerInspect(ctx, name)
		if err != nil {
			return err
		}
		if !info.State.Running || info.RestartCount > 0 {
			return fmt.Errorf("%s exited at startup: %s", name, lastLogLine(ctx, cli, name))
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// lastLogLine reports why a sidecar stopped. The exporter names the cause on
// its final line, which is otherwise lost inside a restart loop.
func lastLogLine(ctx context.Context, cli *client.Client, name string) string {
	text, err := cluster.ContainerLogsText(ctx, cli, name, "")
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return lines[len(lines)-1]
}

// Stop removes the telemetry sidecar. A missing container is not an error, so
// a deployment tears down the same way whether or not telemetry ran.
func Stop(ctx context.Context, cli *client.Client, node string) error {
	err := cli.ContainerRemove(ctx, SidecarName(node), container.RemoveOptions{Force: true})
	if client.IsErrNotFound(err) {
		return nil
	}
	return err
}

// StartAll runs the sidecar on every host of a deployment. Every node is
// attempted even after one fails, because a node without telemetry still
// proves.
func StartAll(ctx context.Context, cfg cluster.Telemetry, hosts *cluster.Hosts, out *cluster.Output) error {
	if !cfg.On() {
		return nil
	}
	var errs []error
	for _, destination := range hosts.Destinations() {
		node := cluster.HostName(destination)
		out.Printf("[%s] starting telemetry sidecar", node)
		if err := Start(ctx, hosts.Client(destination), node, cfg.Interval()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// StopAll removes the sidecar from every host. Failures are ignored, because
// a leftover sidecar must not keep a cluster from coming down.
func StopAll(ctx context.Context, hosts *cluster.Hosts) {
	for _, destination := range hosts.Destinations() {
		_ = Stop(ctx, hosts.Client(destination), cluster.HostName(destination))
	}
}
