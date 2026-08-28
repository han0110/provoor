// Package supervisor runs the ZisK coordinator and ends it on request.
//
// The coordinator holds the set of guest programs already set up in memory and
// never removes an entry, replaying all of them to every worker that
// registers. Two entries mean two setups in a row on one worker process, which
// corrupts the earlier guest's assembly. Ending the coordinator is the only way
// to clear that record, so a client about to serve a different guest asks for
// it first and the container's restart policy starts the replacement.
//
// It sits apart from the zisk package because the binary that runs it ships
// inside the cluster image, where the verifier that package links through cgo
// has no place.
package supervisor

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// Verb is the line a request has to carry. The deployment's own readiness
	// probe writes a newline to this port, so anything but the verb has to
	// leave a running proof alone.
	Verb = "restart"

	// readTimeout bounds how long a connection may hold a handler without
	// sending its verb.
	readTimeout = 5 * time.Second
	// terminateGrace is what the coordinator gets to leave on its own before
	// it is killed. The daemon runs its own grace in parallel on a stop, so
	// escalating outside that window would leave the daemon killing the
	// container instead, without the coordinator's own shutdown.
	terminateGrace = 5 * time.Second
)

// Run starts command, ends it on a request from listener or on a signal, and
// returns the exit code the container carries.
func Run(command []string, listener net.Listener) (int, error) {
	// The coordinator takes SIGTERM cleanly and exits zero, which no restart
	// policy acts on. Ending it on request therefore has to mark the container
	// as failed itself, or the replacement never starts.
	var requested atomic.Bool

	cmd := exec.Command(command[0], command[1:]...) //nolint:gosec // the command is the container's own
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("starting %s: %w", command[0], err)
	}

	// A stop of the container has to reach the coordinator rather than only
	// this process, or the daemon waits out its grace and kills it uncleanly.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		end(cmd.Process)
	}()

	// ended is closed once the coordinator is gone, which is what a request
	// holds its connection open for.
	ended := make(chan struct{})
	go serve(listener, cmd.Process, &requested, ended)

	status := exitCode(cmd.Wait())
	close(ended)
	if requested.Load() && status == 0 {
		fmt.Println("coordinator ended on request")
		status = 1
	}
	return status, nil
}

// serve ends the coordinator on every request that carries the verb.
func serve(listener net.Listener, process *os.Process, requested *atomic.Bool, ended <-chan struct{}) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go answer(conn, process, requested, ended)
	}
}

// answer handles one connection. It writes only when it refuses and closes
// only once the coordinator has exited, so a caller reading nothing back has
// the record cleared by the time it reconnects.
func answer(conn net.Conn, process *os.Process, requested *atomic.Bool, ended <-chan struct{}) {
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(readTimeout))
	line, err := bufio.NewReader(io.LimitReader(conn, 64)).ReadString('\n')
	if err != nil {
		return
	}
	if strings.TrimSpace(line) != Verb {
		_, _ = io.WriteString(conn, "expected the verb "+Verb+"\n")
		return
	}
	requested.Store(true)
	end(process)
	<-ended
}

// end asks the coordinator to leave and kills it if it will not.
func end(process *os.Process) {
	_ = process.Signal(syscall.SIGTERM)
	time.AfterFunc(terminateGrace, func() { _ = process.Kill() })
}

// exitCode turns a wait error into the code the container carries, reporting a
// signalled coordinator the way a shell does so the cause survives.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return 1
	}
	if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal())
	}
	return exit.ExitCode()
}
