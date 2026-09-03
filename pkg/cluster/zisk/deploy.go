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

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/telemetry"
)

// Readiness budgets. The coordinator only has to open two ports, while a
// worker initialises its GPUs before dialing in, so registration gets the
// longer budget.
const (
	coordinatorReadyTimeout = 120 * time.Second
	registrationTimeout     = 300 * time.Second
)

// Up compiles every configured guest into each worker host's artifact cache,
// then deploys the cluster and blocks until the coordinator accepts
// connections and every worker has registered. It is idempotent, a container
// already running is left in place and a proving-key volume already present is
// reused.
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
	for i, worker := range cfg.Workers {
		g.Go(func() error {
			return d.ensureProvingKey(gctx, hosts.Client(worker.SSH), worker, cluster.WorkerName(i, worker.GPU))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Compile every guest into each worker host's artifact cache. Hosts run in
	// parallel and the guests of one host run in turn, since a setup allocates
	// the whole device.
	guests, err := d.resolveGuests(ctx)
	if err != nil {
		return err
	}
	g, gctx = errgroup.WithContext(ctx)
	for i, worker := range cfg.Workers {
		g.Go(func() error {
			return d.setupGuests(gctx, worker, cluster.WorkerName(i, worker.GPU), guests)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if err := d.startCoordinator(ctx); err != nil {
		return err
	}

	g, gctx = errgroup.WithContext(ctx)
	for i, worker := range cfg.Workers {
		g.Go(func() error {
			return d.startWorker(gctx, worker, cluster.WorkerName(i, worker.GPU))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// A cluster without GPU metrics still proves, so telemetry reports its
	// own failure and leaves the deployment up.
	if err := telemetry.StartAll(ctx, cfg.Telemetry, hosts, out); err != nil {
		out.Printf("telemetry unavailable: %v", err)
	}

	out.Printf("cluster ready, coordinator api on %s port %d, %d workers registered, %d guests compiled",
		cfg.coordinatorHost(), apiPort, len(cfg.Workers), len(cfg.Guests))
	return nil
}

// coordinatorHost names the machine the coordinator runs on, which its own
// progress lines leave out. It reports where the API answers and which host a
// container failure has to be chased on.
func (cfg *Config) coordinatorHost() string {
	return cluster.HostName(cfg.Coordinator.SSH)
}

// waitRegistered blocks until a worker registers with the coordinator. The
// log is read from the container's own start time, which the daemon stamps on
// the same clock as the lines themselves and which a restart moves forward,
// so an earlier run's registration is never mistaken for this one's.
func (d *deployment) waitRegistered(ctx context.Context, worker Worker, name string) error {
	cli := d.hosts.Client(worker.SSH)
	deadline := time.Now().Add(registrationTimeout)
	for {
		inspect, err := cli.ContainerInspect(ctx, workerContainer)
		if err != nil {
			return fmt.Errorf("inspecting %s: %w", name, err)
		}
		if inspect.State == nil || !inspect.State.Running {
			return fmt.Errorf("%s exited before registering, journalctl CONTAINER_NAME=%s has the log",
				name, workerContainer)
		}
		logs, err := cluster.ContainerLogsText(ctx, cli, workerContainer, inspect.State.StartedAt)
		if err != nil {
			return fmt.Errorf("reading the log of %s: %w", name, err)
		}
		if strings.Contains(logs, registrationLine) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not register after %s", name, registrationTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// Down stops and removes every cluster container, workers first and then the
// coordinator, reversing the start order. Stop failures are collected rather
// than aborting, so one degraded host cannot leave the coordinator running.
// Journald logs and the proving-key and cache volumes stay in place.
func (cfg *Config) Down(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	telemetry.StopAll(ctx, hosts)

	var (
		mu   sync.Mutex
		errs []error
	)
	var wg sync.WaitGroup
	for i, worker := range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cluster.StopAndRemove(ctx, hosts.Client(worker.SSH), workerContainer, cluster.WorkerName(i, worker.GPU), out); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := cluster.StopAndRemove(ctx, hosts.Client(cfg.Coordinator.SSH), coordinatorContainer, cluster.CoordinatorName, out); err != nil {
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

func (d *deployment) ensureProvingKey(ctx context.Context, cli *client.Client, worker Worker, node string) error {
	volumeName := provingKeyVolume(d.cfg.ZkvmVersion)
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

	containerCfg, hostCfg := setupSpec(d.cfg, worker.GPU)
	output := d.out.Prefixed(node)
	err := cluster.RunToCompletion(ctx, cli, setupContainer, containerCfg, hostCfg, nil, output)
	output.Flush()
	if err != nil {
		// A failed run removes the partial volume, so the next Up retries.
		// The volume standing in for a finished setup, a removal that does
		// not take leaves the next Up starting workers against an empty key
		// directory, so the failure is reported rather than dropped.
		if removeErr := cli.VolumeRemove(context.WithoutCancel(ctx), volumeName, true); removeErr != nil {
			return fmt.Errorf("proving-key setup on %s: %w, and removing the partial volume %s failed, remove it before retrying: %v",
				node, err, volumeName, removeErr)
		}
		return fmt.Errorf("proving-key setup on %s: %w", node, err)
	}
	d.out.Printf("[%s] proving key ready in volume %s", node, volumeName)
	return nil
}

func (d *deployment) startCoordinator(ctx context.Context) error {
	cli := d.hosts.Client(d.cfg.Coordinator.SSH)
	node := d.cfg.coordinatorHost()

	running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, coordinatorContainer)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", node, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", cluster.CoordinatorName, coordinatorContainer)
	} else {
		containerCfg, hostCfg := coordinatorSpec(d.cfg)
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, coordinatorContainer)
		if err != nil {
			return fmt.Errorf("creating coordinator on %s: %w", node, err)
		}
		toml := coordinatorTOML(d.cfg.Workers)
		if err := cluster.CopyFileToContainer(ctx, cli, created.ID, coordinatorConfig, []byte(toml)); err != nil {
			return fmt.Errorf("writing coordinator config on %s: %w", node, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", node, err)
		}
		d.out.Printf("[%s] starting coordinator (api %d, cluster %d)...", cluster.CoordinatorName, apiPort, clusterPort)
	}

	if err := cluster.WaitContainerListening(ctx, cli, coordinatorContainer, node, coordinatorReadyTimeout, apiPort, clusterPort, coordinatorRestartPort); err != nil {
		return err
	}
	d.out.Printf("[%s] coordinator ready (api %d, cluster %d)", cluster.CoordinatorName, apiPort, clusterPort)
	return nil
}

func (d *deployment) startWorker(ctx context.Context, worker Worker, name string) error {
	cli := d.hosts.Client(worker.SSH)

	running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, workerContainer)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", name, workerContainer)
	} else {
		numaNodes, err := d.hostTopology(ctx, cli, name)
		if err != nil {
			return err
		}
		containerCfg, hostCfg := workerSpec(d.cfg, worker, name, numaNodes)
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, workerContainer)
		if err != nil {
			return fmt.Errorf("creating %s: %w", name, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting %s: %w", name, err)
		}
		d.out.Printf("[%s] starting worker, waiting for registration...", name)
	}

	if err := d.waitRegistered(ctx, worker, name); err != nil {
		return err
	}
	d.out.Printf("[%s] worker registered", name)
	return nil
}

// resolvedGuest carries one guest's artifacts, read once on this machine and
// copied into every host's setup container.
type resolvedGuest struct {
	name         string
	elf          []byte
	verifyingKey []byte
}

// resolveGuests reads every configured guest's ELF and verifying key, so a
// missing or unreachable artifact fails the deployment before any host starts
// compiling.
func (d *deployment) resolveGuests(ctx context.Context) ([]resolvedGuest, error) {
	guests := make([]resolvedGuest, len(d.cfg.Guests))
	for i, guest := range d.cfg.Guests {
		name := cluster.GuestELFName(guest.ELF)
		elf, verifyingKey, err := cluster.ResolveGuest(ctx, guest)
		if err != nil {
			return nil, fmt.Errorf("resolving guest %s: %w", name, err)
		}
		guests[i] = resolvedGuest{name: name, elf: elf, verifyingKey: verifyingKey}
	}
	return guests, nil
}

// setupGuests compiles every guest into one host's artifact cache, so the
// first setup a forwarder asks for is a cache hit rather than the minutes of
// ROM and assembly generation it would otherwise pay inside a measured run.
// The setup also derives the guest's verifying key and checks it against the
// configured one, which fails the deployment rather than leaving a cluster
// whose proofs no verifier would accept.
func (d *deployment) setupGuests(ctx context.Context, worker Worker, node string, guests []resolvedGuest) error {
	cli := d.hosts.Client(worker.SSH)
	containerCfg, hostCfg := programSetupSpec(d.cfg, worker.GPU)
	for _, guest := range guests {
		d.out.Printf("[%s] compiling guest %s...", node, guest.name)
		files := map[string][]byte{guestELFPath: guest.elf, guestVKPath: guest.verifyingKey}
		output := d.out.Prefixed(node)
		err := cluster.RunToCompletion(ctx, cli, programSetupContainer, containerCfg, hostCfg, files, output)
		output.Flush()
		if err != nil {
			return fmt.Errorf("compiling guest %s on %s: %w", guest.name, node, err)
		}
		d.out.Printf("[%s] guest %s cached", node, guest.name)
	}
	return nil
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
