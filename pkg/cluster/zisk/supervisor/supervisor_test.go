package supervisor

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
// coordinator's, so the supervisor tests need no external command.
const helperMarker = "stand-in-coordinator"

// TestHelperCoordinator is not a test. It is the stand-in coordinator the
// supervisor runs, re-executed from this binary. It records its start, then
// waits until it is ended, exits with a code, or ignores SIGTERM so the kill
// escalation is exercised.
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

	// The real coordinator takes SIGTERM cleanly and exits zero, so the
	// stand-in has to as well or the supervisor is never asked to mark the
	// container failed itself.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
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
		os.Args[0], "-test.run=^TestHelperCoordinator$", "-test.timeout=0",
		"--", helperMarker, mode, marker,
	}
}

// running starts a supervisor over the stand-in and returns its address and a
// channel carrying the exit code.
func running(t *testing.T, mode string) (address string, done chan int) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "marker")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	done = make(chan int, 1)
	go func() {
		status, _ := Run(helperCommand(mode, marker), listener)
		done <- status
	}()

	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(marker); err == nil {
			return listener.Addr().String(), done
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the stand-in coordinator never started")
	return "", nil
}

// ask speaks the protocol a forwarder speaks and returns what came back, which
// is empty when the request landed.
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

// ended waits for the supervisor to return, so a test never reads a status
// that has not settled.
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

// A request ends the coordinator, which ends the supervisor, which is what
// lets the container's restart policy start the replacement.
func TestVerbEndsTheCoordinator(t *testing.T) {
	address, done := running(t, "run")
	if reply := ask(t, address, Verb); reply != "" {
		t.Fatalf("reply = %q, want nothing back on a request that landed", reply)
	}
	if status := ended(t, done); status == 0 {
		t.Error("status = 0, the restart policy only replaces a container that failed")
	}
}

func TestOnlyTheVerbEndsIt(t *testing.T) {
	address, done := running(t, "run")

	if reply := ask(t, address, "hello"); !strings.Contains(reply, Verb) {
		t.Errorf("reply = %q, want the verb named", reply)
	}
	// A bare connection carries no verb, so the handler answers nothing and
	// the coordinator is left alone.
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

	// The coordinator is still running, so end it rather than leave it holding
	// this binary's output past the run.
	ask(t, address, Verb)
	ended(t, done)
}

// A coordinator that fails on its own carries its own code out, so an operator
// reading the container sees why it went.
func TestCrashCarriesItsCode(t *testing.T) {
	_, done := running(t, "crash")
	if status := ended(t, done); status != 7 {
		t.Errorf("status = %d, want 7", status)
	}
}

func TestStubbornCoordinatorIsKilled(t *testing.T) {
	address, done := running(t, "stubborn")
	if reply := ask(t, address, Verb); reply != "" {
		t.Fatalf("reply = %q, want nothing back", reply)
	}
	if status := ended(t, done); status != 128+int(syscall.SIGKILL) {
		t.Errorf("status = %d, want %d, a coordinator that ignores the signal has to be killed",
			status, 128+int(syscall.SIGKILL))
	}
}
