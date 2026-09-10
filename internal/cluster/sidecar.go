package cluster

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// The DCGM sidecar publishes GPU counters from an embedded host engine, or
// from the node's own nvidia-dcgm.service when the configuration names it.
// Release 4.8.3 is the floor, the first that names the cumulative
// DCGM_FI_PROF_*_TOTAL fields, and NVIDIA publishes it for DCGM 4.6 only as
// a distroless image. The node sidecar publishes processor and memory
// counters and needs nothing from the host.
const (
	dcgmImage      = "nvcr.io/nvidia/k8s/dcgm-exporter:4.6.0-4.8.3-distroless"
	dcgmPort       = 9401
	dcgmFieldsPath = "/etc/dcgm-exporter/provoor.csv"
	nodeImage      = "quay.io/prometheus/node-exporter:v1.12.1"
	nodePort       = 9402
	// sidecarSettle is how long a new sidecar is watched before it counts as
	// up. An exporter that cannot serve exits within a second.
	sidecarSettle = 5 * time.Second
)

// dcgmFields lists the DCGM fields to publish, one per line as a name, a
// Prometheus type, and help text. The type reaches the scraper, which
// subtracts counters and averages gauges.
//
//go:embed dcgm-fields.csv
var dcgmFields []byte

// sidecarPrefix names the container of each kind.
var sidecarPrefix = map[string]string{
	sidecarDCGM: "provoor-dcgm-",
	sidecarNode: "provoor-node-",
}

// StartSidecars runs every configured sidecar. Every one is attempted even
// after one fails, because a node without telemetry still proves.
func StartSidecars(ctx context.Context, cfg Telemetry, hosts *Hosts, out *Output) error {
	if len(cfg.Sidecars) == 0 {
		out.Printf("telemetry: no sidecars configured")
		return nil
	}
	var errs []error
	for _, sidecar := range cfg.Sidecars {
		node := HostName(sidecar.SSH)
		out.Printf("[%s] starting %s sidecar", node, sidecar.Kind)
		if err := startSidecar(ctx, hosts.Client(sidecar.SSH), sidecar, node, cfg.interval()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// StopSidecars removes every kind of sidecar from every host. Failures are
// ignored, so a leftover sidecar never keeps a cluster from coming down.
func StopSidecars(ctx context.Context, hosts *Hosts) {
	for _, destination := range hosts.Destinations() {
		for kind := range sidecarPrefix {
			_ = stopSidecar(ctx, hosts.Client(destination), kind, HostName(destination))
		}
	}
}

// Sidecars lists the telemetry containers a configuration deploys.
func Sidecars(cfg Telemetry) []Deployed {
	deployed := make([]Deployed, len(cfg.Sidecars))
	for i, sidecar := range cfg.Sidecars {
		deployed[i] = Deployed{
			SSH:     sidecar.SSH,
			Name:    sidecarName(sidecar.Kind, HostName(sidecar.SSH)),
			Label:   sidecar.Kind,
			Sidecar: true,
		}
	}
	return deployed
}

// sidecarName labels one node's sidecar of one kind. Characters Docker
// rejects in a container name become a dash.
func sidecarName(kind, node string) string {
	var b strings.Builder
	b.WriteString(sidecarPrefix[kind])
	for _, r := range node {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// startSidecar runs one sidecar on one node, replacing a leftover from an
// earlier deployment so the field set always matches this build.
func startSidecar(ctx context.Context, cli *client.Client, sidecar Sidecar, node string, interval time.Duration) error {
	kind := sidecar.Kind
	spec := sidecarSpec(sidecar, interval)
	spec.Name = sidecarName(kind, node)
	spec.HostConfig.LogConfig = Journald(spec.Name)
	// A pull failure is fatal only when the host has no copy already.
	if pullErr := pullImage(ctx, cli, spec.Config.Image); pullErr != nil {
		if _, err := cli.ImageInspect(ctx, spec.Config.Image); err != nil {
			return fmt.Errorf("pulling the %s image on %s: %w", kind, node, pullErr)
		}
	}
	_ = cli.ContainerRemove(ctx, spec.Name, container.RemoveOptions{Force: true})
	if err := spec.Start(ctx, cli); err != nil {
		return fmt.Errorf("starting %s: %w", spec.Name, err)
	}
	return waitSidecarRunning(ctx, cli, spec.Name)
}

// stopSidecar removes one sidecar, tolerating one already gone.
func stopSidecar(ctx context.Context, cli *client.Client, kind, node string) error {
	err := cli.ContainerRemove(ctx, sidecarName(kind, node), container.RemoveOptions{Force: true})
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

// sidecarSpec builds one sidecar. Host networking reaches a loopback DCGM
// engine and publishes the port on the node's addresses. Only a DCGM sidecar
// with an embedded engine takes the GPUs, SYS_ADMIN for the profiling
// fields, and root for the driver device nodes.
func sidecarSpec(sidecar Sidecar, interval time.Duration) Container {
	hostConfig := &container.HostConfig{
		NetworkMode:   "host",
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		CapDrop:       []string{"ALL"},
		SecurityOpt:   []string{"no-new-privileges"},
	}
	if sidecar.Kind == sidecarNode {
		hostConfig.ReadonlyRootfs = true
		return Container{
			Config: &container.Config{
				Image: nodeImage,
				Cmd: []string{
					"--web.listen-address=:" + strconv.Itoa(nodePort),
					"--collector.disable-defaults",
					"--collector.cpu",
					"--collector.meminfo",
				},
			},
			HostConfig: hostConfig,
		}
	}
	cmd := []string{
		"-f", dcgmFieldsPath,
		"-a", ":" + strconv.Itoa(dcgmPort),
		"-c", strconv.Itoa(int(interval.Milliseconds())),
		// The startup check runs ldconfig, which the distroless image
		// lacks, and skipping it lets the sidecar run with no GPU.
		"--disable-startup-validate",
		// Node identity comes from the scraper, so no hostname reaches
		// published results.
		"--no-hostname",
	}
	user := ""
	if sidecar.NVHostEngine != "" {
		cmd = append(cmd, "-r", sidecar.NVHostEngine)
		user = "65534:65534"
	} else {
		hostConfig.CapAdd = []string{"SYS_ADMIN"}
		hostConfig.Resources.DeviceRequests = []container.DeviceRequest{{Count: -1, Capabilities: [][]string{{"gpu"}}}}
	}
	// The root filesystem stays writable because Docker refuses to extract
	// the field list into a read-only container.
	return Container{
		Config: &container.Config{
			Image: dcgmImage,
			User:  user,
			Cmd:   cmd,
		},
		HostConfig: hostConfig,
		Files:      map[string][]byte{dcgmFieldsPath: dcgmFields},
	}
}

// waitSidecarRunning reports a sidecar that exits at startup, which the
// restart policy would otherwise turn into a silent crash loop.
func waitSidecarRunning(ctx context.Context, cli *client.Client, name string) error {
	deadline := time.Now().Add(sidecarSettle)
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
		if err := Sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

func lastLogLine(ctx context.Context, cli *client.Client, name string) string {
	text, err := containerLogs(ctx, cli, name, "")
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return lines[len(lines)-1]
}
