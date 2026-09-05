package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/cli/cli/connhelper"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"golang.org/x/sync/errgroup"
)

const (
	listeningPollInterval = 3 * time.Second
	logPollInterval       = 5 * time.Second
	// containerRemoveTimeout bounds the cleanup of a one-off container, which
	// runs after the invocation context is done.
	containerRemoveTimeout = 30 * time.Second
)

// Hosts holds the dialed daemons of one deployment, one per SSH destination.
type Hosts struct {
	clients map[string]*client.Client
}

// Container is one container to create on a daemon.
type Container struct {
	Name       string
	Config     *container.Config
	HostConfig *container.HostConfig
	// Files are written into the container before it starts, so no file
	// touches the remote host filesystem.
	Files map[string][]byte
}

// DialHosts dials every unique destination and checks that each daemon
// answers.
func DialHosts(ctx context.Context, destinations []string) (*Hosts, error) {
	h := &Hosts{clients: map[string]*client.Client{}}
	for _, destination := range destinations {
		if _, ok := h.clients[destination]; ok {
			continue
		}
		cli, err := dial(destination)
		if err != nil {
			h.Close()
			return nil, err
		}
		h.clients[destination] = cli
		if _, err := cli.Ping(ctx); err != nil {
			h.Close()
			return nil, fmt.Errorf("docker daemon on %s is unreachable: %w", HostName(destination), err)
		}
	}
	return h, nil
}

// Client returns the daemon dialed for a destination.
func (h *Hosts) Client(destination string) *client.Client {
	return h.clients[destination]
}

// Destinations lists the unique dialed destinations.
func (h *Hosts) Destinations() []string {
	destinations := make([]string, 0, len(h.clients))
	for destination := range h.clients {
		destinations = append(destinations, destination)
	}
	return destinations
}

// Close closes every dialed daemon.
func (h *Hosts) Close() {
	for _, cli := range h.clients {
		_ = cli.Close()
	}
}

// Pull pulls an image on every host in parallel.
func (h *Hosts) Pull(ctx context.Context, ref string, out *Output) error {
	g, ctx := errgroup.WithContext(ctx)
	for destination, cli := range h.clients {
		g.Go(func() error {
			out.Printf("[%s] pulling %s", HostName(destination), ref)
			return pullImage(ctx, cli, ref)
		})
	}
	return g.Wait()
}

// dial connects to the Docker daemon behind an SSH destination through the
// local ssh binary, so the user's SSH configuration, agent, and proxies apply.
// An empty destination uses the local environment's daemon and a URL of any
// other scheme passes through.
func dial(destination string) (*client.Client, error) {
	url := daemonURL(destination)
	options := []client.Opt{client.WithAPIVersionNegotiation()}
	switch {
	case url == "":
		options = append(options, client.FromEnv)
	case strings.HasPrefix(url, "ssh://"):
		helper, err := connhelper.GetConnectionHelper(url)
		if err != nil {
			return nil, fmt.Errorf("ssh destination %q: %w", destination, err)
		}
		options = append(options, client.WithHost(helper.Host), client.WithDialContext(helper.Dialer))
	default:
		options = append(options, client.WithHost(url))
	}
	cli, err := client.NewClientWithOpts(options...)
	if err != nil {
		return nil, fmt.Errorf("creating docker client for %q: %w", destination, err)
	}
	return cli, nil
}

// daemonURL turns a bare SSH destination into an ssh URL and passes an
// explicit URL through.
func daemonURL(destination string) string {
	if destination == "" || strings.Contains(destination, "://") {
		return destination
	}
	return "ssh://" + destination
}

// HostName derives a node name from an SSH destination by dropping the
// scheme, the user, and the port.
func HostName(destination string) string {
	if destination == "" {
		return "local"
	}
	host := strings.TrimPrefix(destination, "ssh://")
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	if colon := strings.LastIndex(host, ":"); colon >= 0 {
		host = host[:colon]
	}
	return host
}

// RustLog is the RUST_LOG level a verbosity level selects.
func RustLog(verbose int) string {
	switch verbose {
	case 0:
		return "info"
	case 1:
		return "debug"
	default:
		return "trace"
	}
}

// Journald logs a container to the journal under its name as tag.
func Journald(name string) container.LogConfig {
	return container.LogConfig{Type: "journald", Config: map[string]string{"tag": name}}
}

// MemlockUnlimited removes the locked-memory limit, so a container can pin
// the memory a GPU prover holds.
func MemlockUnlimited() []*container.Ulimit {
	return []*container.Ulimit{{Name: "memlock", Soft: -1, Hard: -1}}
}

// DeviceRequest exposes the GPU selection to a container.
func (g GPU) DeviceRequest() container.DeviceRequest {
	request := container.DeviceRequest{Capabilities: [][]string{{"gpu"}}}
	request.DeviceIDs = make([]string, len(g.DeviceIDs))
	for i, id := range g.DeviceIDs {
		request.DeviceIDs[i] = strconv.Itoa(id)
	}
	return request
}

// pullImage pulls an image, consuming the progress stream.
func pullImage(ctx context.Context, cli *client.Client, ref string) error {
	reader, err := cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pulling %s: %w", ref, err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return fmt.Errorf("pulling %s: %w", ref, err)
	}
	return nil
}

// Running reports whether the named container is running, removing a stopped
// or restarting leftover so the name is free.
func Running(ctx context.Context, cli *client.Client, name string) (bool, error) {
	inspect, err := cli.ContainerInspect(ctx, name)
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Docker reports a container in its restart backoff as running, so a
	// crash loop counts as absent.
	if inspect.State.Running && !inspect.State.Restarting {
		return true, nil
	}
	return false, cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
}

// coordinatorReadyTimeout bounds the wait for the coordinator ports, which
// open with no GPU work ahead of them.
const coordinatorReadyTimeout = 120 * time.Second

// StartCoordinator starts the coordinator unless it is already running and
// waits until every listening port accepts connections. portLabel names them
// in the progress lines.
func StartCoordinator(ctx context.Context, cli *client.Client, node string, spec Container, portLabel string, listening []int, out *Output) error {
	running, err := Running(ctx, cli, spec.Name)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", node, err)
	}
	if running {
		out.Printf("[%s] %s already running, run provoor down first to apply config changes", CoordinatorName, spec.Name)
	} else {
		if err := spec.Start(ctx, cli); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", node, err)
		}
		out.Printf("[%s] starting coordinator (%s)...", CoordinatorName, portLabel)
	}
	if err := waitListening(ctx, cli, spec.Name, node, listening...); err != nil {
		return err
	}
	out.Printf("[%s] coordinator ready (%s)", CoordinatorName, portLabel)
	return nil
}

// Start creates the container, writes its files, and starts it.
func (c Container) Start(ctx context.Context, cli *client.Client) error {
	created, err := cli.ContainerCreate(ctx, c.Config, c.HostConfig, nil, nil, c.Name)
	if err != nil {
		return err
	}
	for path, data := range c.Files {
		if err := copyFile(ctx, cli, created.ID, path, data); err != nil {
			return err
		}
	}
	return cli.ContainerStart(ctx, created.ID, container.StartOptions{})
}

// Run runs the container to completion, streams its output, and removes it
// afterwards. A leftover with the same name is removed first.
func (c Container) Run(ctx context.Context, cli *client.Client, output io.Writer) error {
	_ = cli.ContainerRemove(ctx, c.Name, container.RemoveOptions{Force: true})
	defer func() {
		removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), containerRemoveTimeout)
		defer cancel()
		_ = cli.ContainerRemove(removeCtx, c.Name, container.RemoveOptions{Force: true})
	}()
	if err := c.Start(ctx, cli); err != nil {
		return err
	}

	logs, err := cli.ContainerLogs(ctx, c.Name, container.LogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		return err
	}
	streamed := make(chan struct{})
	go func() {
		defer close(streamed)
		_, _ = stdcopy.StdCopy(output, output, logs)
	}()
	// Closing the stream unblocks the copier, so no write to output outlives
	// this call.
	defer func() {
		_ = logs.Close()
		<-streamed
	}()

	statusCh, errCh := cli.ContainerWait(ctx, c.Name, container.WaitConditionNotRunning)
	select {
	case status := <-statusCh:
		<-streamed
		if status.StatusCode != 0 {
			return fmt.Errorf("exited with code %d", status.StatusCode)
		}
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// copyFile writes data into a created container through the archive API. The
// archive is extracted at the root with the full path as the entry name, which
// creates the parent directories the image lacks.
func copyFile(ctx context.Context, cli *client.Client, containerID, path string, data []byte) error {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	header := &tar.Header{Name: strings.TrimPrefix(path, "/"), Mode: 0o644, Size: int64(len(data))}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return cli.CopyToContainer(ctx, containerID, "/", &buf, container.CopyToContainerOptions{})
}

// waitListening polls until every port accepts a connection inside the
// container, failing early when the container exits first.
func waitListening(ctx context.Context, cli *client.Client, name, node string, ports ...int) error {
	dials := make([]string, len(ports))
	for i, port := range ports {
		dials[i] = fmt.Sprintf("(echo > /dev/tcp/127.0.0.1/%d)", port)
	}
	probe := []string{"bash", "-c", strings.Join(dials, " && ")}
	ctx, cancel := context.WithTimeout(ctx, coordinatorReadyTimeout)
	defer cancel()
	// expired separates the budget running out from the caller cancelling.
	expired := func() error {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%s on %s not ready after %s", name, node, coordinatorReadyTimeout)
		}
		return ctx.Err()
	}
	for {
		inspect, err := cli.ContainerInspect(ctx, name)
		if err != nil {
			if ctx.Err() != nil {
				return expired()
			}
			return fmt.Errorf("inspecting %s on %s: %w", name, node, err)
		}
		if !inspect.State.Running {
			return fmt.Errorf("%s on %s exited before it was ready, journalctl CONTAINER_NAME=%s has the log", name, node, name)
		}
		ready, err := execSucceeds(ctx, cli, name, probe)
		if err != nil {
			if ctx.Err() != nil {
				return expired()
			}
			return fmt.Errorf("probing %s on %s: %w", name, node, err)
		}
		if ready {
			return nil
		}
		if err := Sleep(ctx, listeningPollInterval); err != nil {
			return expired()
		}
	}
}

// WaitLogLine polls the container log until it carries line, failing when the
// container exits or the timeout passes. The log is read from the container's
// own start time, so a restart never replays an earlier run's line.
func WaitLogLine(ctx context.Context, cli *client.Client, name, label, line string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		inspect, err := cli.ContainerInspect(ctx, name)
		if err != nil {
			return fmt.Errorf("inspecting %s: %w", label, err)
		}
		if !inspect.State.Running {
			return fmt.Errorf("%s exited before it was ready, journalctl CONTAINER_NAME=%s has the log", label, name)
		}
		logs, err := containerLogs(ctx, cli, name, inspect.State.StartedAt)
		if err != nil {
			return fmt.Errorf("reading the log of %s: %w", label, err)
		}
		if strings.Contains(logs, line) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready after %s", label, timeout)
		}
		if err := Sleep(ctx, logPollInterval); err != nil {
			return err
		}
	}
}

// execSucceeds runs a command inside a running container and reports whether
// it exited zero.
func execSucceeds(ctx context.Context, cli *client.Client, name string, cmd []string) (bool, error) {
	exec, err := cli.ContainerExecCreate(ctx, name, container.ExecOptions{Cmd: cmd})
	if err != nil {
		return false, err
	}
	if err := cli.ContainerExecStart(ctx, exec.ID, container.ExecStartOptions{}); err != nil {
		return false, err
	}
	for {
		inspect, err := cli.ContainerExecInspect(ctx, exec.ID)
		if err != nil {
			return false, err
		}
		if !inspect.Running {
			return inspect.ExitCode == 0, nil
		}
		if err := Sleep(ctx, 200*time.Millisecond); err != nil {
			return false, err
		}
	}
}

// StopAndRemove stops and removes a container, tolerating one already gone.
func StopAndRemove(ctx context.Context, cli *client.Client, name, node string, out *Output) error {
	err := cli.ContainerStop(ctx, name, container.StopOptions{})
	if errdefs.IsNotFound(err) {
		out.Printf("[%s] %s not found", node, name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("stopping %s on %s: %w", name, node, err)
	}
	if err := cli.ContainerRemove(ctx, name, container.RemoveOptions{}); err != nil {
		return fmt.Errorf("removing %s on %s: %w", name, node, err)
	}
	out.Printf("[%s] %s stopped and removed", node, name)
	return nil
}

// containerLogs returns a container's combined output as text, starting at
// since.
func containerLogs(ctx context.Context, cli *client.Client, name, since string) (string, error) {
	logs, err := cli.ContainerLogs(ctx, name, container.LogsOptions{ShowStdout: true, ShowStderr: true, Since: since})
	if err != nil {
		return "", err
	}
	defer func() { _ = logs.Close() }()
	var buf bytes.Buffer
	if _, err := stdcopy.StdCopy(&buf, &buf, logs); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Sleep waits for d or until ctx ends, and reports which.
func Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
