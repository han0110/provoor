package ereserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"

	"github.com/han0110/provoor/internal/cluster"
)

const (
	// containerPrefix names one estimation container. The port makes it
	// unique, so estimations of several guests run side by side.
	containerPrefix = "provoor-estimate-"
	// logLabel prefixes every line the container prints.
	logLabel = "ere-server"
	// healthPollInterval paces the wait for the server to load the guest.
	healthPollInterval = 2 * time.Second
	// healthRequestTimeout drops a stalled probe, so the poll keeps its pace.
	healthRequestTimeout = 10 * time.Second
	// startTimeout bounds the whole bring-up, sized for the preprocessing a
	// zkVM runs before it serves.
	startTimeout = 20 * time.Minute
	// removeTimeout bounds the cleanup, which runs after the caller's context
	// may already be done.
	removeTimeout = 30 * time.Second
)

// Server is a local ere-server container that holds one guest program.
type Server struct {
	Client
	docker   *client.Client
	name     string
	attached types.HijackedResponse
	output   *cluster.PrefixWriter
	streamed chan struct{}
}

// Start runs an ere-server on the guest program elf and waits until it
// serves. The container publishes a free loopback port and reads the ELF from
// its stdin, so the ELF never reaches the host filesystem. The caller closes
// the returned server.
func Start(ctx context.Context, docker *client.Client, image string, elf []byte, output *cluster.Output) (*Server, error) {
	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("reserving a port for %s: %w", image, err)
	}
	name := containerPrefix + strconv.Itoa(port)
	created, err := docker.ContainerCreate(ctx, containerConfig(image, port), hostConfig(port), nil, nil, name)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", name, err)
	}
	// Attaching before the start keeps the first log lines, and carries the
	// ELF on the same connection.
	attached, err := docker.ContainerAttach(ctx, created.ID, container.AttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		_ = removeContainer(ctx, docker, name)
		return nil, fmt.Errorf("attaching to %s: %w", name, err)
	}
	s := &Server{
		Client:   Client{BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port), HTTP: &http.Client{}},
		docker:   docker,
		name:     name,
		attached: attached,
		output:   output.Prefixed(logLabel),
		streamed: make(chan struct{}),
	}
	go func() {
		defer close(s.streamed)
		_, _ = stdcopy.StdCopy(s.output, s.output, attached.Reader)
	}()
	if err := s.feed(ctx, created.ID, elf); err != nil {
		_ = s.Close(ctx)
		return nil, err
	}
	if err := s.waitHealthy(ctx); err != nil {
		_ = s.Close(ctx)
		return nil, err
	}
	return s, nil
}

// Name is the name of the container.
func (s *Server) Name() string { return s.name }

// Close removes the container and stops relaying its output.
func (s *Server) Close(ctx context.Context) error {
	err := removeContainer(ctx, s.docker, s.name)
	s.attached.Close()
	<-s.streamed
	s.output.Flush()
	return err
}

// feed starts the container and hands it the ELF, which it reads from stdin
// until end of file.
func (s *Server) feed(ctx context.Context, containerID string, elf []byte) error {
	if err := s.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting %s: %w", s.name, err)
	}
	if _, err := s.attached.Conn.Write(elf); err != nil {
		return fmt.Errorf("writing the guest program to %s: %w", s.name, err)
	}
	if err := s.attached.CloseWrite(); err != nil {
		return fmt.Errorf("closing the stdin of %s: %w", s.name, err)
	}
	return nil
}

// waitHealthy polls until the server answers, which takes as long as the zkVM
// needs to load the guest program.
func (s *Server) waitHealthy(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	for {
		inspect, err := s.docker.ContainerInspect(ctx, s.name)
		if errdefs.IsNotFound(err) || (err == nil && !inspect.State.Running) {
			return fmt.Errorf("%s exited before it served the guest program", s.name)
		}
		if err != nil {
			return fmt.Errorf("inspecting %s: %w", s.name, err)
		}
		probe, cancelProbe := context.WithTimeout(ctx, healthRequestTimeout)
		err = s.Health(probe)
		cancelProbe()
		if err == nil {
			return nil
		}
		if err := cluster.Sleep(ctx, healthPollInterval); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%s did not serve the guest program in %s", s.name, startTimeout)
			}
			return err
		}
	}
}

// containerConfig runs the server on the CPU, with the ELF arriving on a
// stdin that closes once it is written.
func containerConfig(image string, port int) *container.Config {
	return &container.Config{
		Image:        image,
		Cmd:          []string{"--port", strconv.Itoa(port), "cpu"},
		ExposedPorts: nat.PortSet{containerPort(port): {}},
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		OpenStdin:    true,
		StdinOnce:    true,
	}
}

// hostConfig publishes the port on loopback only, and lets the daemon reap
// the container as soon as it exits.
func hostConfig(port int) *container.HostConfig {
	return &container.HostConfig{
		AutoRemove: true,
		PortBindings: nat.PortMap{
			containerPort(port): {{HostIP: "127.0.0.1", HostPort: strconv.Itoa(port)}},
		},
	}
}

func containerPort(port int) nat.Port {
	return nat.Port(strconv.Itoa(port) + "/tcp")
}

// freePort takes a loopback port the kernel reports as free and releases it
// for the container to bind.
func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// removeContainer stops and removes a container, tolerating one the daemon
// already reaped.
func removeContainer(ctx context.Context, docker *client.Client, name string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), removeTimeout)
	defer cancel()
	err := docker.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}
