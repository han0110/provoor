package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// Hosts carries the dialed daemons of one deployment, keyed by SSH
// destination so a host serving several roles is dialed once.
type Hosts struct {
	clients map[string]*client.Client
}

// DialHosts dials every unique destination and verifies each daemon responds.
func DialHosts(ctx context.Context, destinations []string) (*Hosts, error) {
	h := &Hosts{clients: map[string]*client.Client{}}
	for _, destination := range destinations {
		if _, ok := h.clients[destination]; ok {
			continue
		}
		cli, err := Dial(destination)
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

// PullImage pulls an image, consuming the progress stream.
func PullImage(ctx context.Context, cli *client.Client, ref string) error {
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

// RunToCompletion runs a one-off container, streams its output, and removes
// it afterwards. A leftover container with the same name is removed first,
// and files are written into the created container before it starts.
func RunToCompletion(ctx context.Context, cli *client.Client, name string, containerCfg *container.Config, hostCfg *container.HostConfig, files map[string][]byte, output io.Writer) error {
	_ = cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, name)
	if err != nil {
		return err
	}
	defer func() {
		_ = cli.ContainerRemove(context.WithoutCancel(ctx), created.ID, container.RemoveOptions{Force: true})
	}()
	for filePath, data := range files {
		if err := CopyFileToContainer(ctx, cli, created.ID, filePath, data); err != nil {
			return err
		}
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return err
	}

	logs, err := cli.ContainerLogs(ctx, created.ID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Follow: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = logs.Close() }()
	streamed := make(chan struct{})
	go func() {
		defer close(streamed)
		_, _ = stdcopy.StdCopy(output, output, logs)
	}()

	statusCh, errCh := cli.ContainerWait(ctx, created.ID, container.WaitConditionNotRunning)
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

// ExecSucceeds runs a command inside a running container and reports whether
// it exited zero.
func ExecSucceeds(ctx context.Context, cli *client.Client, name string, cmd []string) (bool, error) {
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
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// EnsureAbsentUnlessRunning reports whether the named container is already
// running, removing a stopped leftover so the name is free.
func EnsureAbsentUnlessRunning(ctx context.Context, cli *client.Client, name string) (bool, error) {
	inspect, err := cli.ContainerInspect(ctx, name)
	if client.IsErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if inspect.State != nil && inspect.State.Running {
		return true, nil
	}
	if err := cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true}); err != nil {
		return false, err
	}
	return false, nil
}

// StopAndRemove stops and removes a container, tolerating one already gone.
func StopAndRemove(ctx context.Context, cli *client.Client, name, node string, out *Output) error {
	err := cli.ContainerStop(ctx, name, container.StopOptions{})
	if client.IsErrNotFound(err) {
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

// ContainerLogsText returns a container's combined output as text.
func ContainerLogsText(ctx context.Context, cli *client.Client, name string) (string, error) {
	logs, err := cli.ContainerLogs(ctx, name, container.LogsOptions{ShowStdout: true, ShowStderr: true})
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

// CopyFileToContainer writes data into a created container at filePath
// through the archive API, so no file touches the remote host filesystem.
// The archive is extracted at the root with the full path as the entry name,
// which creates parent directories the image lacks.
func CopyFileToContainer(ctx context.Context, cli *client.Client, containerID, filePath string, data []byte) error {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	header := &tar.Header{Name: strings.TrimPrefix(filePath, "/"), Mode: 0o644, Size: int64(len(data))}
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
