// Command zisk-supervisor runs the ZisK coordinator and ends it on request.
// The coordinator never drops a guest it has set up and replays every one to
// each registering worker, so ending it is the only way to clear that record.
package main

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
	// verb is the line a restart request carries. The readiness probe writes
	// a bare newline to the same port, so anything else leaves the
	// coordinator alone.
	verb = "restart"
	// readTimeout bounds a connection that sends no verb.
	readTimeout = 5 * time.Second
	// terminateGrace is the time the coordinator gets to exit on SIGTERM
	// before it is killed. The daemon runs its own grace in parallel on a
	// stop, so a longer window leaves the daemon killing the container.
	terminateGrace = 5 * time.Second
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: zisk-supervisor <coordinator command>")
		os.Exit(2)
	}
	port := os.Getenv("RESTART_PORT")
	if port == "" {
		fmt.Fprintln(os.Stderr, "RESTART_PORT must name the port restart requests arrive on")
		os.Exit(2)
	}

	listener, err := net.Listen("tcp", net.JoinHostPort("", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "listening for restart requests: %v\n", err)
		os.Exit(1)
	}
	status, err := run(os.Args[1:], listener)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(status)
}

// run starts command, ends it on a request from listener or on a signal, and
// returns the exit code the container carries. A requested end of a clean
// exit reports 1, since the restart policy replaces only a failed container.
func run(command []string, listener net.Listener) (int, error) {
	var requested atomic.Bool

	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("starting %s: %w", command[0], err)
	}

	// A stop of the container reaches the coordinator through this process,
	// or the daemon waits out its grace and kills it uncleanly.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	ended := make(chan struct{})
	go func() {
		select {
		case <-signals:
			end(cmd.Process)
		case <-ended:
		}
	}()
	go serve(listener, cmd.Process, &requested, ended)

	status := exitCode(cmd.Wait())
	close(ended)
	if requested.Load() && status == 0 {
		fmt.Println("coordinator ended on request")
		status = 1
	}
	return status, nil
}

// serve answers every connection until the listener closes.
func serve(listener net.Listener, process *os.Process, requested *atomic.Bool, ended <-chan struct{}) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go answer(conn, process, requested, ended)
	}
}

// answer handles one connection. It writes only on a refusal and closes only
// once the coordinator has exited, so a caller reading end of stream finds
// the record cleared when it reconnects.
func answer(conn net.Conn, process *os.Process, requested *atomic.Bool, ended <-chan struct{}) {
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(readTimeout))
	line, err := bufio.NewReader(io.LimitReader(conn, 64)).ReadString('\n')
	if err != nil {
		return
	}
	if strings.TrimSpace(line) != verb {
		_, _ = io.WriteString(conn, "expected the verb "+verb+"\n")
		return
	}
	requested.Store(true)
	end(process)
	<-ended
}

// end asks the coordinator to exit and kills it after terminateGrace.
func end(process *os.Process) {
	_ = process.Signal(syscall.SIGTERM)
	time.AfterFunc(terminateGrace, func() { _ = process.Kill() })
}

// exitCode turns a wait error into the code the container carries, reporting
// a signalled coordinator as 128 plus the signal the way a shell does.
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
