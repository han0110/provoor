package openvm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFramedStdin(t *testing.T) {
	framed := framedStdin([]byte{0xAA, 0xBB, 0xCC})
	want := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		0xAA, 0xBB, 0xCC,
		0, 0, 0, 0, 0, 0, 0, 0,
	}
	if !bytes.Equal(framed, want) {
		t.Errorf("framed = %x, want %x", framed, want)
	}
}

func TestNewProofUUID(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{32}$`)
	first, second := newProofUUID(), newProofUUID()
	if !pattern.MatchString(first) {
		t.Errorf("uuid = %q", first)
	}
	if first == second {
		t.Errorf("uuids must differ, both %q", first)
	}
}

func TestClassifyStartError(t *testing.T) {
	if err := classifyStartError(http.StatusConflict, `{"error":"program_not_in_loadout","message":"..."}`); errors.Is(err, errClusterBusy) || errors.Is(err, errClusterNotReady) {
		t.Errorf("loadout rejection must be fatal, got %v", err)
	}
	if err := classifyStartError(http.StatusConflict, `{"error":"Another proof is already running"}`); !errors.Is(err, errClusterBusy) {
		t.Errorf("busy = %v", err)
	}
	if err := classifyStartError(http.StatusServiceUnavailable, `{"error":"workers not ready"}`); !errors.Is(err, errClusterNotReady) {
		t.Errorf("unavailable = %v", err)
	}
	if err := classifyStartError(http.StatusInternalServerError, `1 of 2 workers failed to accept work: x`); !errors.Is(err, errClusterNotReady) {
		t.Errorf("dispatch failure = %v", err)
	}
	if err := classifyStartError(http.StatusInternalServerError, `no input staged for proof`); errors.Is(err, errClusterNotReady) {
		t.Errorf("other 500s must be fatal, got %v", err)
	}
}

func TestParseProofStatus(t *testing.T) {
	for _, tt := range []struct {
		data, status, reason string
	}{
		{`"in_progress"`, "in_progress", ""},
		{`"completed"`, "completed", ""},
		{`"canceled"`, "canceled", ""},
		{`{"failing":"worker died"}`, "failing", "worker died"},
		{`{"failed":"worker died"}`, "failed", "worker died"},
	} {
		status, reason, err := parseProofStatus(tt.data)
		if err != nil || status != tt.status || reason != tt.reason {
			t.Errorf("parseProofStatus(%s) = %q, %q, %v", tt.data, status, reason, err)
		}
	}
	if _, _, err := parseProofStatus(`not json`); err == nil {
		t.Error("expected an error for malformed status")
	}
}

// fakeCoordinator serves the client-facing endpoints of one proof, with the
// status stream scripted per subscription.
type fakeCoordinator struct {
	t     *testing.T
	input []byte
	proof []byte
	// streams scripts each successive event-stream subscription, every
	// entry one SSE payload written before the handler returns.
	streams [][]string
	holdEnd bool

	mu            sync.Mutex
	subscriptions int
	uploaded      []byte
	startedBody   string
	cancelled     bool
}

func (f *fakeCoordinator) server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload_input/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("input")
		if err != nil {
			f.t.Errorf("upload lacks the input part: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if header.Filename != "input.bin" {
			f.t.Errorf("upload filename = %q", header.Filename)
		}
		body, _ := io.ReadAll(file)
		f.mu.Lock()
		f.uploaded = body
		f.mu.Unlock()
		_, _ = w.Write([]byte("Input staged"))
	})
	mux.HandleFunc("POST /start_proof", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.startedBody = string(body)
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"status":"started"}`))
	})
	mux.HandleFunc("GET /proof_events/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		index := f.subscriptions
		f.subscriptions++
		f.mu.Unlock()
		if index >= len(f.streams) {
			index = len(f.streams) - 1
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, data := range f.streams[index] {
			fmt.Fprintf(w, "event: status\ndata: %s\n\n", data)
			flusher.Flush()
		}
		if f.holdEnd {
			<-r.Context().Done()
		}
	})
	mux.HandleFunc("GET /proof_state/{uuid}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"proof_uuid":"x","e2e_latency_ms":17805}`))
	})
	mux.HandleFunc("GET /proof/{uuid}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(f.proof)
	})
	mux.HandleFunc("POST /cancel_proof", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		f.cancelled = true
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"status":"canceled"}`))
	})
	return httptest.NewServer(mux)
}

func TestProve(t *testing.T) {
	proof, wantPublicValues := buildProof(sampleElements(), 0), samplePublicValues()
	fake := &fakeCoordinator{
		t:       t,
		input:   []byte("stateless-input"),
		proof:   proof,
		streams: [][]string{{`"in_progress"`, `"completed"`}},
	}
	server := fake.server()
	defer server.Close()

	var phases []string
	result, err := DialClient(server.URL).Prove(context.Background(), "program-0123456789abcdef", fake.input,
		func(phase string) { phases = append(phases, phase) })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.PublicValues, wantPublicValues) {
		t.Errorf("PublicValues = %x", result.PublicValues)
	}
	if result.ProofBytes != len(proof) {
		t.Errorf("ProofBytes = %d, want %d", result.ProofBytes, len(proof))
	}
	if result.ClusterProvingTime != 17805*time.Millisecond {
		t.Errorf("ClusterProvingTime = %s", result.ClusterProvingTime)
	}
	if len(phases) != 1 || phases[0] != "in_progress" {
		t.Errorf("phases = %v", phases)
	}
	if !bytes.Equal(fake.uploaded, framedStdin(fake.input)) {
		t.Errorf("uploaded = %x", fake.uploaded)
	}
	for _, required := range []string{`"program":{"name":"program-0123456789abcdef","version":0}`, `"input_already_uploaded":false`} {
		if !strings.Contains(fake.startedBody, required) {
			t.Errorf("start_proof body lacks %q: %s", required, fake.startedBody)
		}
	}
}

func TestProveReconnectsDroppedStream(t *testing.T) {
	proof := buildProof(sampleElements(), 0)
	fake := &fakeCoordinator{
		t:     t,
		proof: proof,
		// The first subscription ends unsettled, the replayed second one
		// settles.
		streams: [][]string{{`"in_progress"`}, {`"completed"`}},
	}
	server := fake.server()
	defer server.Close()

	if _, err := DialClient(server.URL).Prove(context.Background(), "program-x", []byte("input"), nil); err != nil {
		t.Fatal(err)
	}
	if fake.subscriptions < 2 {
		t.Errorf("subscriptions = %d, want a reconnect", fake.subscriptions)
	}
}

func TestProveFailure(t *testing.T) {
	fake := &fakeCoordinator{
		t:       t,
		streams: [][]string{{`{"failing":"worker died"}`, `{"failed":"worker died"}`}},
	}
	server := fake.server()
	defer server.Close()

	var phases []string
	_, err := DialClient(server.URL).Prove(context.Background(), "program-x", []byte("input"),
		func(phase string) { phases = append(phases, phase) })
	if err == nil || !strings.Contains(err.Error(), "worker died") {
		t.Errorf("err = %v", err)
	}
	if len(phases) != 1 || phases[0] != "failing" {
		t.Errorf("phases = %v", phases)
	}
}

func TestProveTimeoutCancels(t *testing.T) {
	fake := &fakeCoordinator{
		t:       t,
		streams: [][]string{{`"in_progress"`}},
		holdEnd: true,
	}
	server := fake.server()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := DialClient(server.URL).Prove(ctx, "program-x", []byte("input"), nil)
	var timeoutErr *ProveTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("err = %v, want a ProveTimeoutError", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.cancelled {
		t.Error("timeout must cancel the proof")
	}
}

func TestProveFatalSubmitErrorDoesNotRetry(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload_input/{uuid}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("Input staged"))
	})
	starts := 0
	mux.HandleFunc("POST /start_proof", func(w http.ResponseWriter, _ *http.Request) {
		starts++
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"program_not_in_loadout","message":"Program x@v0 is not in the current loadout"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := DialClient(server.URL).Prove(context.Background(), "program-x", []byte("input"), nil)
	if err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("err = %v", err)
	}
	if starts != 1 {
		t.Errorf("starts = %d, a fatal rejection must not retry", starts)
	}
}

func TestCheckProgram(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("name") != "program-known" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"program 'program-x' is not in the loadout"}`))
			return
		}
		_, _ = w.Write([]byte("baseline"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := DialClient(server.URL)
	if err := client.CheckProgram(context.Background(), "program-known"); err != nil {
		t.Errorf("known program = %v", err)
	}
	if err := client.CheckProgram(context.Background(), "program-x"); err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("unknown program = %v", err)
	}
}

func TestWaitReady(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ready":true,"num_workers":1,"expected_num_workers":1}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if err := DialClient(server.URL).WaitReady(context.Background()); err != nil {
		t.Errorf("WaitReady = %v", err)
	}
}
