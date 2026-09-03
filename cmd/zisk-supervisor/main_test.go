package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// helperMarker separates this binary's own arguments from the stand-in
// coordinator's.
const helperMarker = "stand-in-coordinator"

// endedSuffix names the file the stand-in writes as it leaves, which shows
// the coordinator was gone before a request returned.
const endedSuffix = ".ended"

// helperCoordinatorBudget backstops a stand-in a failing test never ends.
const helperCoordinatorBudget = 2 * time.Minute

// TestHelperCoordinator is the stand-in coordinator, re-executed from this
// binary with a mode that selects a clean exit on SIGTERM, a crash, or
// ignoring SIGTERM.
func TestHelperCoordinator(t *testing.T) {
	mode, marker, ok := helperArgs()
	if !ok {
		t.Skip("driven by the supervisor tests, not run on its own")
	}
	if err := os.WriteFile(marker, []byte("started"), 0o644); err != nil {
		os.Exit(3)
	}

	switch mode {
	case "crash":
		os.Exit(7)
	case "stubborn":
		signal.Ignore(syscall.SIGTERM)
		select {}
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	if err := os.WriteFile(marker+endedSuffix, []byte("ended"), 0o644); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

func helperArgs() (mode, marker string, ok bool) {
	for i, arg := range os.Args {
		if arg == helperMarker && i+2 < len(os.Args) {
			return os.Args[i+1], os.Args[i+2], true
		}
	}
	return "", "", false
}

func helperCommand(mode, marker string) []string {
	return []string{
		os.Args[0], "-test.run=^TestHelperCoordinator$",
		"-test.timeout=" + helperCoordinatorBudget.String(),
		"--", helperMarker, mode, marker,
	}
}

// running starts a supervisor over the stand-in and returns its address and a
// channel carrying the exit code. Cleanup ends the coordinator, so a failing
// test never orphans it.
func running(t *testing.T, mode string) (address string, done chan int, marker string) {
	t.Helper()
	marker = filepath.Join(t.TempDir(), "marker")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	done = make(chan int, 1)
	go func() {
		status, _ := run(helperCommand(mode, marker), listener)
		done <- status
		close(done)
	}()
	address = listener.Addr().String()
	t.Cleanup(func() {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			return
		}
		_, _ = io.WriteString(conn, verb+"\n")
		_ = conn.Close()
		<-done
	})

	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(marker); err == nil {
			return address, done, marker
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the stand-in coordinator never started")
	return "", nil, ""
}

// ask sends one line the way a forwarder does and returns the reply, empty
// when the request landed.
func ask(t *testing.T, address, line string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatalf("dialing the supervisor: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(conn, line+"\n"); err != nil {
		t.Fatalf("sending %q: %v", line, err)
	}
	reply, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(reply)
}

// ended waits for the supervisor to return its status.
func ended(t *testing.T, done chan int) int {
	t.Helper()
	select {
	case status := <-done:
		return status
	case <-time.After(30 * time.Second):
		t.Fatal("the supervisor kept running")
		return 0
	}
}

func TestVerbEndsTheCoordinator(t *testing.T) {
	address, done, marker := running(t, "run")
	if reply := ask(t, address, verb); reply != "" {
		t.Fatalf("reply = %q, want nothing back on a request that landed", reply)
	}
	if status := ended(t, done); status != 1 {
		t.Errorf("status = %d, want 1, the restart policy only replaces a container that failed", status)
	}
	if _, err := os.Stat(marker + endedSuffix); err != nil {
		t.Errorf("the coordinator had not exited when the request returned: %v", err)
	}
}

func TestOnlyTheVerbEndsIt(t *testing.T) {
	address, done, _ := running(t, "run")

	if reply := ask(t, address, "hello"); !strings.Contains(reply, verb) {
		t.Errorf("reply = %q, want the verb named", reply)
	}
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	select {
	case status := <-done:
		t.Fatalf("the coordinator ended with %d, neither a bare connect nor a wrong verb may end it", status)
	case <-time.After(time.Second):
	}

	ask(t, address, verb)
	ended(t, done)
}

func TestCrashCarriesItsCode(t *testing.T) {
	_, done, _ := running(t, "crash")
	if status := ended(t, done); status != 7 {
		t.Errorf("status = %d, want 7", status)
	}
}

func TestStubbornCoordinatorIsKilled(t *testing.T) {
	address, done, _ := running(t, "stubborn")
	if reply := ask(t, address, verb); reply != "" {
		t.Fatalf("reply = %q, want nothing back", reply)
	}
	if status := ended(t, done); status != 128+int(syscall.SIGKILL) {
		t.Errorf("status = %d, want %d, a coordinator that ignores the signal has to be killed",
			status, 128+int(syscall.SIGKILL))
	}
}
