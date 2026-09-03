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

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"golang.org/x/sync/errgroup"

	"github.com/han0110/provoor/internal/cluster"
)

// The coordinator only has to open its ports, while a worker initialises its
// GPUs before dialing in, so registration gets the longer budget.
const (
	coordinatorReadyTimeout = 120 * time.Second
	registrationTimeout     = 300 * time.Second
)

// deployment carries the dialed hosts of one Up invocation.
type deployment struct {
	cfg   *Config
	hosts *cluster.Hosts
	out   *cluster.Output
}

// resolvedGuest carries one guest's artifacts, read once on this machine and
// copied into every host's setup container.
type resolvedGuest struct {
	name         string
	elf          []byte
	verifyingKey []byte
}

// Up prepares every worker host's proving key, compiles every guest into each
// host's artifact cache, then starts the cluster and blocks until the
// coordinator accepts connections and every worker has registered. A container
// already running is left in place.
func (cfg *Config) Up(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()
	d := &deployment{cfg: cfg, hosts: hosts, out: out}

	if err := hosts.Pull(ctx, cfg.ImageRef(), out); err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	for i, worker := range cfg.Workers {
		g.Go(func() error {
			return d.ensureProvingKey(gctx, worker, cluster.WorkerName(i, worker.GPU))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Hosts compile in parallel and the guests of one host in turn, since a
	// setup allocates the whole device.
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

	// A cluster without GPU metrics still proves.
	if err := cluster.StartSidecars(ctx, cfg.Telemetry, hosts, out); err != nil {
		out.Printf("telemetry unavailable: %v", err)
	}

	out.Printf("cluster ready, coordinator api on %s port %d, %d workers registered, %d guests compiled",
		cfg.CoordinatorHost(), apiPort, len(cfg.Workers), len(cfg.Guests))
	return nil
}

// Down stops and removes every cluster container, workers first and then the
// coordinator, collecting stop failures so one degraded host cannot leave the
// coordinator running. Journald logs and the volumes stay in place.
func (cfg *Config) Down(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	cluster.StopSidecars(ctx, hosts)

	errs := make([]error, len(cfg.Workers))
	var wg sync.WaitGroup
	for i, worker := range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cluster.StopAndRemove(ctx, hosts.Client(worker.SSH), workerContainer, cluster.WorkerName(i, worker.GPU), out)
		}()
	}
	wg.Wait()
	errs = append(errs, cluster.StopAndRemove(ctx, hosts.Client(cfg.Coordinator.SSH), coordinatorContainer, cluster.CoordinatorName, out))
	if err := errors.Join(errs...); err != nil {
		return err
	}

	out.Printf("logs remain available via journalctl CONTAINER_NAME=%s or CONTAINER_NAME=%s on each host",
		coordinatorContainer, workerContainer)
	return nil
}

// ensureProvingKey runs the proving-key setup into a host's volume when the
// volume is missing. First use downloads and builds const-trees, so progress
// streams line by line.
func (d *deployment) ensureProvingKey(ctx context.Context, worker Worker, node string) error {
	cli := d.hosts.Client(worker.SSH)
	volumeName := provingKeyVolume(d.cfg.ZkvmVersion)
	if _, err := cli.VolumeInspect(ctx, volumeName); err == nil {
		d.out.Printf("[%s] proving-key volume %s already exists", node, volumeName)
		return nil
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspecting volume %s on %s: %w", volumeName, node, err)
	}

	d.out.Printf("[%s] setting up proving key into volume %s...", node, volumeName)
	if _, err := cli.VolumeCreate(ctx, volume.CreateOptions{Name: volumeName}); err != nil {
		return fmt.Errorf("creating volume %s on %s: %w", volumeName, node, err)
	}

	output := d.out.Prefixed(node)
	err := setupSpec(d.cfg, worker.GPU).Run(ctx, cli, output)
	output.Flush()
	if err != nil {
		// A failed run removes the partial volume, so the next Up retries. A
		// removal that does not take is reported, since the volume otherwise
		// stands in for a finished setup.
		if removeErr := cli.VolumeRemove(context.WithoutCancel(ctx), volumeName, true); removeErr != nil {
			return fmt.Errorf("proving-key setup on %s: %w, and removing the partial volume %s failed, remove it before retrying: %v",
				node, err, volumeName, removeErr)
		}
		return fmt.Errorf("proving-key setup on %s: %w", node, err)
	}
	d.out.Printf("[%s] proving key ready in volume %s", node, volumeName)
	return nil
}

// resolveGuests reads every configured guest's ELF and verifying key, so a
// missing artifact fails the deployment before any host starts compiling.
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
// first setup a forwarder asks for is a cache hit. The setup checks the
// derived verifying key against the configured one and fails the deployment
// on a mismatch.
func (d *deployment) setupGuests(ctx context.Context, worker Worker, node string, guests []resolvedGuest) error {
	cli := d.hosts.Client(worker.SSH)
	for _, guest := range guests {
		d.out.Printf("[%s] compiling guest %s...", node, guest.name)
		output := d.out.Prefixed(node)
		err := programSetupSpec(d.cfg, worker.GPU, guest.elf, guest.verifyingKey).Run(ctx, cli, output)
		output.Flush()
		if err != nil {
			return fmt.Errorf("compiling guest %s on %s: %w", guest.name, node, err)
		}
		d.out.Printf("[%s] guest %s cached", node, guest.name)
	}
	return nil
}

func (d *deployment) startCoordinator(ctx context.Context) error {
	cli := d.hosts.Client(d.cfg.Coordinator.SSH)
	node := d.cfg.CoordinatorHost()

	running, err := cluster.Running(ctx, cli, coordinatorContainer)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", node, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", cluster.CoordinatorName, coordinatorContainer)
	} else {
		if err := coordinatorSpec(d.cfg).Start(ctx, cli); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", node, err)
		}
		d.out.Printf("[%s] starting coordinator (api %d, cluster %d)...", cluster.CoordinatorName, apiPort, clusterPort)
	}

	if err := cluster.WaitListening(ctx, cli, coordinatorContainer, node, coordinatorReadyTimeout, apiPort, clusterPort, coordinatorRestartPort); err != nil {
		return err
	}
	d.out.Printf("[%s] coordinator ready (api %d, cluster %d)", cluster.CoordinatorName, apiPort, clusterPort)
	return nil
}

func (d *deployment) startWorker(ctx context.Context, worker Worker, name string) error {
	cli := d.hosts.Client(worker.SSH)

	running, err := cluster.Running(ctx, cli, workerContainer)
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
		if err := workerSpec(d.cfg, worker, name, numaNodes).Start(ctx, cli); err != nil {
			return fmt.Errorf("starting %s: %w", name, err)
		}
		d.out.Printf("[%s] starting worker, waiting for registration...", name)
	}

	if err := cluster.WaitLogLine(ctx, cli, workerContainer, name, registrationLine, registrationTimeout); err != nil {
		return err
	}
	d.out.Printf("[%s] worker registered", name)
	return nil
}

// hostTopology observes the NUMA node count the rank-mapping derivation
// needs, one node when undetectable. Only the last output line is parsed,
// since the image prints a startup banner.
func (d *deployment) hostTopology(ctx context.Context, cli *client.Client, node string) (int, error) {
	numaNodes := 1
	if d.cfg.Prover.MPINumaPpr != 0 || d.cfg.Prover.MPINp <= 1 {
		return numaNodes, nil
	}
	var output bytes.Buffer
	if err := topologyProbeSpec(d.cfg).Run(ctx, cli, &output); err != nil {
		return 0, fmt.Errorf("detecting NUMA nodes on %s: %w", node, err)
	}
	trimmed := strings.TrimSpace(output.String())
	if count, err := strconv.Atoi(trimmed[strings.LastIndexByte(trimmed, '\n')+1:]); err == nil && count > 1 {
		numaNodes = count
	}
	return numaNodes, nil
}
