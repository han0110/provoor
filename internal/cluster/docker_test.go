package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	// dockerAPIVersion pins the client to one path prefix, so the fake daemon
	// serves fixed routes and answers no negotiation ping.
	dockerAPIVersion = "1.51"
	fakeContainerID  = "0123456789ab"
	// logWriteDelay keeps the log copier inside a write long enough for a
	// cancelled run to return while that write is still in flight.
	logWriteDelay = 50 * time.Millisecond
)

// newFakeDaemon serves routes as a Docker daemon and returns a client dialed
// at it. Route patterns omit the API version prefix the client prepends.
func newFakeDaemon(t *testing.T, routes map[string]http.HandlerFunc) *client.Client {
	t.Helper()
	mux := http.NewServeMux()
	for pattern, handler := range routes {
		method, path, _ := strings.Cut(pattern, " ")
		mux.HandleFunc(method+" /v"+dockerAPIVersion+path, handler)
	}
	server := httptest.NewServer(mux)
	cli, err := client.NewClientWithOpts(client.WithHost(server.URL), client.WithVersion(dockerAPIVersion))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		server.Close()
	})
	return cli
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func respondContainerCreated(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusCreated, container.CreateResponse{ID: fakeContainerID})
}

func respondNoContent(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// stdoutFrame wraps payload in the eight byte header multiplexing stdout into
// the log stream of a container without a TTY.
func stdoutFrame(payload string) []byte {
	frame := make([]byte, 8, 8+len(payload))
	frame[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(frame[4:], uint32(len(payload)))
	return append(frame, payload...)
}

// streamContainerLogs emits frames until the request is cancelled, like a
// followed log stream that outlives the caller's context.
func streamContainerLogs(w http.ResponseWriter, r *http.Request) {
	flusher := w.(http.Flusher)
	for line := 0; ; line++ {
		if _, err := w.Write(stdoutFrame(fmt.Sprintf("line %d\n", line))); err != nil {
			return
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// recordingWriter spends logWriteDelay in every write and reports whether one
// landed after the run that streams into it returned.
type recordingWriter struct {
	mutex            sync.Mutex
	returned         bool
	wroteAfterReturn bool
	started          chan struct{}
}

func (w *recordingWriter) Write(data []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	time.Sleep(logWriteDelay)
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.returned {
		w.wroteAfterReturn = true
	}
	return len(data), nil
}

func (w *recordingWriter) markReturned() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.returned = true
}

func (w *recordingWriter) sawWriteAfterReturn() bool {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.wroteAfterReturn
}

func TestRunWaitsForLogStreamOnCancel(t *testing.T) {
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"POST /containers/create":     respondContainerCreated,
		"POST /containers/{id}/start": respondNoContent,
		"GET /containers/{id}/logs":   streamContainerLogs,
		"POST /containers/{id}/wait": func(w http.ResponseWriter, r *http.Request) {
			// Answering with headers first hands ContainerWait its channels, so
			// the run blocks in its select rather than in the request.
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
		"DELETE /containers/{id}": respondNoContent,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &recordingWriter{started: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() {
		done <- Container{Name: "prover", Config: &container.Config{Image: "image"}, HostConfig: &container.HostConfig{}}.Run(ctx, cli, writer)
	}()

	<-writer.started
	cancel()
	err := <-done
	writer.markReturned()
	if err == nil {
		t.Fatal("err = nil, want the cancellation error")
	}
	time.Sleep(4 * logWriteDelay)
	if writer.sawWriteAfterReturn() {
		t.Error("output was written after Run returned")
	}
}

func TestRunReturnsExitCode(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int64
		wantErr    string
	}{
		{"zero_exit", 0, ""},
		{"non_zero_exit", 3, "exited with code 3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli := newFakeDaemon(t, map[string]http.HandlerFunc{
				"POST /containers/create":     respondContainerCreated,
				"POST /containers/{id}/start": respondNoContent,
				"GET /containers/{id}/logs": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write(stdoutFrame("hello\n"))
				},
				"POST /containers/{id}/wait": func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, container.WaitResponse{StatusCode: tc.statusCode})
				},
				"DELETE /containers/{id}": respondNoContent,
			})

			var output bytes.Buffer
			err := Container{Name: "prover", Config: &container.Config{Image: "image"}, HostConfig: &container.HostConfig{}}.Run(t.Context(), cli, &output)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want mention of %q", err, tc.wantErr)
			}
			if output.String() != "hello\n" {
				t.Errorf("output = %q, want %q", output.String(), "hello\n")
			}
		})
	}
}

func TestRunningTreatsRestartingAsAbsent(t *testing.T) {
	cases := []struct {
		name        string
		state       *container.State
		found       bool
		wantRunning bool
		wantRemoved bool
	}{
		{"running", &container.State{Running: true}, true, true, false},
		{"restarting", &container.State{Running: true, Restarting: true}, true, false, true},
		{"exited", &container.State{}, true, false, true},
		{"absent", nil, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var removed atomic.Bool
			cli := newFakeDaemon(t, map[string]http.HandlerFunc{
				"GET /containers/{name}/json": func(w http.ResponseWriter, r *http.Request) {
					if !tc.found {
						writeJSON(w, http.StatusNotFound, map[string]string{"message": "No such container: " + r.PathValue("name")})
						return
					}
					writeJSON(w, http.StatusOK, container.InspectResponse{
						ContainerJSONBase: &container.ContainerJSONBase{ID: fakeContainerID, State: tc.state},
					})
				},
				"DELETE /containers/{name}": func(w http.ResponseWriter, r *http.Request) {
					removed.Store(true)
					respondNoContent(w, r)
				},
			})

			running, err := Running(t.Context(), cli, "prover")
			if err != nil {
				t.Fatal(err)
			}
			if running != tc.wantRunning {
				t.Errorf("running = %v, want %v", running, tc.wantRunning)
			}
			if removed.Load() != tc.wantRemoved {
				t.Errorf("removed = %v, want %v", removed.Load(), tc.wantRemoved)
			}
		})
	}
}

func TestStartWritesFilesBeforeStarting(t *testing.T) {
	var steps []string
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"POST /containers/create": func(w http.ResponseWriter, r *http.Request) {
			steps = append(steps, "create")
			respondContainerCreated(w, r)
		},
		"PUT /containers/{id}/archive": func(w http.ResponseWriter, r *http.Request) {
			steps = append(steps, "archive")
			respondNoContent(w, r)
		},
		"POST /containers/{id}/start": func(w http.ResponseWriter, r *http.Request) {
			steps = append(steps, "start")
			respondNoContent(w, r)
		},
	})
	err := Container{
		Name:       "prover",
		Config:     &container.Config{Image: "image"},
		HostConfig: &container.HostConfig{},
		Files:      map[string][]byte{"/tmp/config": []byte("x")},
	}.Start(t.Context(), cli)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"create", "archive", "start"}; !reflect.DeepEqual(steps, want) {
		t.Errorf("steps = %v, want %v", steps, want)
	}
}

func TestDaemonURL(t *testing.T) {
	cases := map[string]string{
		"":                            "",
		"node1":                       "ssh://node1",
		"user@203.0.113.1":            "ssh://user@203.0.113.1",
		"ssh://user@203.0.113.1:2222": "ssh://user@203.0.113.1:2222",
		"unix:///var/run/docker.sock": "unix:///var/run/docker.sock",
		"tcp://203.0.113.1:2375":      "tcp://203.0.113.1:2375",
	}
	for destination, want := range cases {
		if got := daemonURL(destination); got != want {
			t.Errorf("daemonURL(%q) = %q, want %q", destination, got, want)
		}
	}
}

func TestHostName(t *testing.T) {
	cases := map[string]string{
		"":                            "local",
		"node1":                       "node1",
		"user@203.0.113.1":            "203.0.113.1",
		"ssh://user@203.0.113.1:2222": "203.0.113.1",
	}
	for destination, want := range cases {
		if got := HostName(destination); got != want {
			t.Errorf("HostName(%q) = %q, want %q", destination, got, want)
		}
	}
}

func TestPrefixWriter(t *testing.T) {
	cases := []struct {
		name           string
		writes         []string
		wantAfterEach  []string
		wantAfterFlush string
	}{
		{"split_line", []string{"partial ", "line\n"}, []string{"", "[node1] partial line\n"}, "[node1] partial line\n"},
		{"carriage_returns_trimmed", []string{"first\r\nsecond\r\n"}, []string{"[node1] first\n[node1] second\n"}, "[node1] first\n[node1] second\n"},
		{"trailing_partial_line", []string{"no newline"}, []string{""}, "[node1] no newline\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewOutput(&buf).Prefixed("node1")
			for index, data := range tc.writes {
				if n, err := writer.Write([]byte(data)); n != len(data) || err != nil {
					t.Fatalf("Write(%q) = %d, %v, want %d, nil", data, n, err, len(data))
				}
				if got := buf.String(); got != tc.wantAfterEach[index] {
					t.Errorf("output after write %d = %q, want %q", index, got, tc.wantAfterEach[index])
				}
			}
			writer.Flush()
			writer.Flush()
			if got := buf.String(); got != tc.wantAfterFlush {
				t.Errorf("output after flush = %q, want %q", got, tc.wantAfterFlush)
			}
		})
	}
}
