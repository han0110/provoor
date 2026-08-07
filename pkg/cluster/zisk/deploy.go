package zisk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/provoor/pkg/cluster"
)

// Readiness budgets. The coordinator only has to open two ports, while a
// worker initialises its GPUs before dialing in, so registration gets the
// longer budget.
const (
	coordinatorReadyTimeout = 120 * time.Second
	registrationTimeout     = 300 * time.Second
)

// Up deploys the cluster and blocks until the coordinator accepts
// connections and every worker has registered. It is idempotent, a container
// already running is left in place and a proving-key volume already present
// is reused.
func (cfg *Config) Up(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()
	d := &deployment{cfg: cfg, hosts: hosts, out: out}

	// Pull the image on every host first, so every container of this
	// deployment runs the current tag.
	g, gctx := errgroup.WithContext(ctx)
	for _, destination := range hosts.Destinations() {
		g.Go(func() error {
			out.Printf("[%s] pulling %s", cluster.HostName(destination), cfg.imageRef())
			return cluster.PullImage(gctx, hosts.Client(destination), cfg.imageRef())
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Prepare each worker host's proving-key volume when missing. First use
	// downloads and builds const-trees, slow enough to look like a hang, so
	// progress streams line by line.
	g, gctx = errgroup.WithContext(ctx)
	for _, worker := range cfg.Workers {
		g.Go(func() error {
			return d.ensureProvingKey(gctx, hosts.Client(worker.SSH), worker.Name)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if err := d.startCoordinator(ctx); err != nil {
		return err
	}

	g, gctx = errgroup.WithContext(ctx)
	for _, worker := range cfg.Workers {
		g.Go(func() error {
			return d.startWorker(gctx, worker)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	out.Printf("cluster ready, coordinator api on %s port %d, %d workers registered",
		cfg.Coordinator.Name, apiPort, len(cfg.Workers))
	return nil
}

// Down stops and removes every cluster container, workers first and then the
// coordinator, reversing the start order. Stop failures are collected rather
// than aborting, so one degraded host cannot leave the coordinator running.
// Journald logs and the proving-key volumes stay in place.
func (cfg *Config) Down(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	var (
		mu   sync.Mutex
		errs []error
	)
	var wg sync.WaitGroup
	for _, worker := range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cluster.StopAndRemove(ctx, hosts.Client(worker.SSH), workerContainer, worker.Name, out); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := cluster.StopAndRemove(ctx, hosts.Client(cfg.Coordinator.SSH), coordinatorContainer, cfg.Coordinator.Name, out); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	out.Printf("logs remain available via journalctl CONTAINER_NAME=%s or CONTAINER_NAME=%s on each host",
		coordinatorContainer, workerContainer)
	return nil
}

func (cfg *Config) destinations() []string {
	destinations := []string{cfg.Coordinator.SSH}
	for _, worker := range cfg.Workers {
		destinations = append(destinations, worker.SSH)
	}
	return destinations
}

// deployment carries the dialed hosts of one Up invocation.
type deployment struct {
	cfg   *Config
	hosts *cluster.Hosts
	out   *cluster.Output
}

func (d *deployment) ensureProvingKey(ctx context.Context, cli *client.Client, node string) error {
	volumeName := provingKeyVolume(d.cfg.ImageTag)
	if _, err := cli.VolumeInspect(ctx, volumeName); err == nil {
		d.out.Printf("[%s] proving-key volume %s already exists", node, volumeName)
		return nil
	} else if !client.IsErrNotFound(err) {
		return fmt.Errorf("inspecting volume %s on %s: %w", volumeName, node, err)
	}

	d.out.Printf("[%s] setting up proving key into volume %s...", node, volumeName)
	if _, err := cli.VolumeCreate(ctx, volume.CreateOptions{Name: volumeName}); err != nil {
		return fmt.Errorf("creating volume %s on %s: %w", volumeName, node, err)
	}

	containerCfg, hostCfg := setupSpec(d.cfg)
	output := d.out.Prefixed(node)
	err := cluster.RunToCompletion(ctx, cli, setupContainer, containerCfg, hostCfg, nil, output)
	output.Flush()
	if err != nil {
		// A failed run removes the partial volume, so the next Up retries.
		_ = cli.VolumeRemove(context.WithoutCancel(ctx), volumeName, true)
		return fmt.Errorf("proving-key setup on %s: %w", node, err)
	}
	d.out.Printf("[%s] proving key ready in volume %s", node, volumeName)
	return nil
}

func (d *deployment) startCoordinator(ctx context.Context) error {
	cli := d.hosts.Client(d.cfg.Coordinator.SSH)
	node := d.cfg.Coordinator.Name

	running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, coordinatorContainer)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", node, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", node, coordinatorContainer)
	} else {
		containerCfg, hostCfg := coordinatorSpec(d.cfg)
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, coordinatorContainer)
		if err != nil {
			return fmt.Errorf("creating coordinator on %s: %w", node, err)
		}
		toml := coordinatorTOML(len(d.cfg.Workers))
		if err := cluster.CopyFileToContainer(ctx, cli, created.ID, coordinatorConfig, []byte(toml)); err != nil {
			return fmt.Errorf("writing coordinator config on %s: %w", node, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", node, err)
		}
		d.out.Printf("[%s] starting coordinator (api %d, cluster %d)...", node, apiPort, clusterPort)
	}

	if err := d.waitCoordinatorReady(ctx, cli, node); err != nil {
		return err
	}
	d.out.Printf("[%s] coordinator ready (api %d, cluster %d)", node, apiPort, clusterPort)
	return nil
}

func (d *deployment) waitCoordinatorReady(ctx context.Context, cli *client.Client, node string) error {
	probe := []string{"bash", "-c", fmt.Sprintf(
		"(echo > /dev/tcp/127.0.0.1/%d) && (echo > /dev/tcp/127.0.0.1/%d)",
		apiPort, clusterPort)}
	deadline := time.Now().Add(coordinatorReadyTimeout)
	for {
		inspect, err := cli.ContainerInspect(ctx, coordinatorContainer)
		if err != nil {
			return fmt.Errorf("inspecting coordinator on %s: %w", node, err)
		}
		if inspect.State == nil || !inspect.State.Running {
			return fmt.Errorf("coordinator on %s exited before becoming ready, journalctl CONTAINER_NAME=%s has the log",
				node, coordinatorContainer)
		}
		ready, err := cluster.ExecSucceeds(ctx, cli, coordinatorContainer, probe)
		if err != nil {
			return fmt.Errorf("probing coordinator on %s: %w", node, err)
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator on %s not ready after %s", node, coordinatorReadyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func (d *deployment) startWorker(ctx context.Context, worker Worker) error {
	cli := d.hosts.Client(worker.SSH)

	running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, workerContainer)
	if err != nil {
		return fmt.Errorf("worker on %s: %w", worker.Name, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", worker.Name, workerContainer)
	} else {
		numaNodes, err := d.hostTopology(ctx, cli, worker.Name)
		if err != nil {
			return err
		}
		containerCfg, hostCfg, err := workerSpec(d.cfg, worker, numaNodes)
		if err != nil {
			return err
		}
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, workerContainer)
		if err != nil {
			return fmt.Errorf("creating worker on %s: %w", worker.Name, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting worker on %s: %w", worker.Name, err)
		}
		d.out.Printf("[%s] starting worker, waiting for registration...", worker.Name)
	}

	deadline := time.Now().Add(registrationTimeout)
	for {
		inspect, err := cli.ContainerInspect(ctx, workerContainer)
		if err != nil {
			return fmt.Errorf("inspecting worker on %s: %w", worker.Name, err)
		}
		if inspect.State == nil || !inspect.State.Running {
			return fmt.Errorf("worker on %s exited before registration, journalctl CONTAINER_NAME=%s has the log",
				worker.Name, workerContainer)
		}
		logs, err := cluster.ContainerLogsText(ctx, cli, workerContainer)
		if err != nil {
			return fmt.Errorf("reading worker log on %s: %w", worker.Name, err)
		}
		if strings.Contains(logs, registrationLine) {
			d.out.Printf("[%s] worker registered", worker.Name)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("worker on %s not registered after %s", worker.Name, registrationTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// hostTopology observes the NUMA node count the rank-mapping derivation
// needs, one probe container reading /sys, falling back to one node when
// undetectable. Only the last output line is parsed, since the image prints
// a startup banner.
func (d *deployment) hostTopology(ctx context.Context, cli *client.Client, node string) (int, error) {
	numaNodes := 1
	if d.cfg.Config.MPINumaPpr != 0 || d.cfg.Config.MPINp <= 1 {
		return numaNodes, nil
	}

	containerCfg := &container.Config{
		Image: d.cfg.imageRef(),
		Cmd:   []string{"bash", "-c", "ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l"},
	}
	var output bytes.Buffer
	if err := cluster.RunToCompletion(ctx, cli, workerContainer+"-probe", containerCfg, &container.HostConfig{}, nil, &output); err != nil {
		return 0, fmt.Errorf("detecting NUMA nodes on %s: %w", node, err)
	}
	trimmed := strings.TrimSpace(output.String())
	if count, err := strconv.Atoi(trimmed[strings.LastIndexByte(trimmed, '\n')+1:]); err == nil && count > 1 {
		numaNodes = count
	}
	return numaNodes, nil
}
