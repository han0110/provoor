package openvm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/han0110/provoor/pkg/ereverifier"
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
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(programVerifyingKeyFixture(f.t))
	})
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

// testProgramName stands in for the guest ELF's content digest the loadout
// is keyed by.
const testProgramName = "program-0123456789abcdef"

// dialFake binds a client to the fake coordinator under the fixture verifying
// key, which every downloaded proof is checked against.
func dialFake(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := DialClient(context.Background(), endpoint, testProgramName, programVerifyingKeyFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// requireUndecodableProof asserts the rejection the stand-in proof earns.
func requireUndecodableProof(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ereverifier.ErrDecodeProof) || !strings.Contains(err.Error(), "decoding the proof envelope") {
		t.Fatalf("err = %v, want a proof decoding failure", err)
	}
}

// TestProve drives one proof through submission, the status stream, and the
// download. The stand-in proof does not verify, so the run ends in the
// verifier's rejection.
func TestProve(t *testing.T) {
	fake := &fakeCoordinator{
		t:       t,
		input:   []byte("stateless-input"),
		proof:   unverifiableProof,
		streams: [][]string{{`"in_progress"`, `"completed"`}},
	}
	server := fake.server()
	defer server.Close()

	var phases []string
	_, err := dialFake(t, server.URL).Prove(context.Background(), fake.input,
		func(phase string) { phases = append(phases, phase) })
	requireUndecodableProof(t, err)
	if len(phases) != 1 || phases[0] != "in_progress" {
		t.Errorf("phases = %v", phases)
	}
	if !bytes.Equal(fake.uploaded, framedStdin(fake.input)) {
		t.Errorf("uploaded = %x", fake.uploaded)
	}
	for _, required := range []string{`"program":{"name":"` + testProgramName + `","version":0}`, `"input_already_uploaded":false`} {
		if !strings.Contains(fake.startedBody, required) {
			t.Errorf("start_proof body lacks %q: %s", required, fake.startedBody)
		}
	}
}

// TestProveVerifiedOutcome drives the same path with the real proof fixture,
// so the outcome a completed proof reports is measured against a proof that
// actually verifies.
func TestProveVerifiedOutcome(t *testing.T) {
	proof := readFixture(t, proofFixture)
	fake := &fakeCoordinator{
		t:       t,
		input:   []byte("stateless-input"),
		proof:   proof,
		streams: [][]string{{`"completed"`}},
	}
	server := fake.server()
	defer server.Close()

	outcome, err := dialFake(t, server.URL).Prove(context.Background(), fake.input, nil)
	if err != nil {
		t.Fatalf("Prove() = %v, want a verified proof", err)
	}
	if want := readFixture(t, publicValuesFixture); !bytes.Equal(outcome.PublicValues, want) {
		t.Errorf("public values = %x, want %x", outcome.PublicValues, want)
	}
	if outcome.ProofBytes != len(proof) {
		t.Errorf("proof bytes = %d, want %d", outcome.ProofBytes, len(proof))
	}
	if want := 17805 * time.Millisecond; outcome.ClusterProvingTime != want {
		t.Errorf("cluster proving time = %s, want %s", outcome.ClusterProvingTime, want)
	}
}

func TestProveReconnectsDroppedStream(t *testing.T) {
	fake := &fakeCoordinator{
		t:     t,
		proof: unverifiableProof,
		// The first subscription ends unsettled, the replayed second one
		// settles.
		streams: [][]string{{`"in_progress"`}, {`"completed"`}},
	}
	server := fake.server()
	defer server.Close()

	_, err := dialFake(t, server.URL).Prove(context.Background(), []byte("input"), nil)
	requireUndecodableProof(t, err)
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
	_, err := dialFake(t, server.URL).Prove(context.Background(), []byte("input"),
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
	_, err := dialFake(t, server.URL).Prove(ctx, []byte("input"), nil)
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
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(programVerifyingKeyFixture(t))
	})
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

	_, err := dialFake(t, server.URL).Prove(context.Background(), []byte("input"), nil)
	if err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("err = %v", err)
	}
	if starts != 1 {
		t.Errorf("starts = %d, a fatal rejection must not retry", starts)
	}
}

func TestDialClient(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("name") != testProgramName {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"program 'program-x' is not in the loadout"}`))
			return
		}
		_, _ = w.Write(programVerifyingKeyFixture(t))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := DialClient(context.Background(), server.URL, testProgramName, programVerifyingKeyFixture(t))
	if err != nil {
		t.Fatalf("known program = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if _, err := DialClient(context.Background(), server.URL, "program-x", programVerifyingKeyFixture(t)); err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("unknown program = %v", err)
	}
	if _, err := DialClient(context.Background(), server.URL, testProgramName, []byte("baseline")); !errors.Is(err, ereverifier.ErrDecodeProgramVK) {
		t.Errorf("malformed verifying key = %v", err)
	}
}

// TestDialClientRejectsClusterVerifyingKeyMismatch covers the cross-check that
// leaves the configured key, not the cluster's, deciding what a proof is
// about, here against a cluster serving another guest's baseline.
func TestDialClientRejectsClusterVerifyingKeyMismatch(t *testing.T) {
	served := readFixture(t, otherBaselineFixture)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(served)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	expected := programVerifyingKeyFixture(t)
	_, err := DialClient(context.Background(), server.URL, testProgramName, expected)
	if err == nil {
		t.Fatal("DialClient() = nil, want a rejected cluster verifying key")
	}
	for _, key := range [][]byte{served, expected} {
		digest := sha256.Sum256(key)
		if !strings.Contains(err.Error(), hex.EncodeToString(digest[:])) {
			t.Errorf("err = %v, want the digest of both keys", err)
		}
	}
}

func TestWaitReady(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(programVerifyingKeyFixture(t))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ready":true,"num_workers":1,"expected_num_workers":1}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if err := dialFake(t, server.URL).WaitReady(context.Background()); err != nil {
		t.Errorf("WaitReady = %v", err)
	}
}
