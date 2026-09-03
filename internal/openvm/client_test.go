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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/han0110/provoor/internal/ereverifier"
)

// The fixtures are a real proof of the warmup block, produced by a cluster
// running the stateless-validator-reth-openvm-v2.1.0-preview guest of
// ere-guests v0.15.0. They carry the verification baseline the coordinator
// serves for it and the public values it proves. other-baseline.bin is the baseline
// of the ethrex guest of the same release, a program the proof does not
// attest to.
const (
	proofFixture         = "proof.bin"
	baselineFixture      = "baseline.bin"
	otherBaselineFixture = "other-baseline.bin"
	publicValuesFixture  = "public-values.bin"
)

// testELF stands in for the guest ELF whose content digest keys the loadout.
var testELF = []byte("test")

// unverifiableProof stands in for a proof envelope. The verifier rejects it
// at the decode step.
var unverifiableProof = []byte("not-an-encoded-proof")

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
	// drain answers the first readiness poll and the first submission the way
	// a manager still holding an aborted dispatch's slot does, cleared by
	// whichever of the two observes it first.
	drain bool
	// readyzReportsDrain decides whether the drain shows on /readyz or the
	// coordinator reports ready throughout it.
	readyzReportsDrain bool

	mu            sync.Mutex
	subscriptions int
	uploaded      []byte
	startedBody   string
	cancelled     bool
}

func (f *fakeCoordinator) server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("name") != programName(testELF) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"program 'program-x' is not in the loadout"}`))
			return
		}
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
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		draining := f.drain && f.readyzReportsDrain
		if draining {
			f.drain = false
		}
		f.mu.Unlock()
		if draining {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("workers draining an earlier proof"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /start_proof", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.startedBody = string(body)
		refuse := f.drain
		f.drain = false
		f.mu.Unlock()
		if refuse {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("another proof is already running"))
			return
		}
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

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

// programVerifyingKeyFixture is a guest program's verification baseline, the
// bytes the coordinator serves per program name.
func programVerifyingKeyFixture(t *testing.T) []byte {
	t.Helper()
	return readFixture(t, baselineFixture)
}

func boundToFixture(t *testing.T, programVerifyingKey []byte) *ereverifier.Verifier {
	t.Helper()
	verifier, err := ereverifier.New(ereverifier.OpenVM, programVerifyingKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifier.Close)
	return verifier
}

// dialFakeCoordinator binds a client to the fake coordinator under the fixture
// verifying key, which every downloaded proof is checked against.
func dialFakeCoordinator(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := Dial(t.Context(), endpoint, testELF, programVerifyingKeyFixture(t))
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
	if err := classifyStartError(http.StatusInternalServerError, `Failed to upload input to workers: x`); !errors.Is(err, errClusterNotReady) {
		t.Errorf("fan-out failure = %v", err)
	}
	if err := classifyStartError(http.StatusInternalServerError, `no input staged for proof`); errors.Is(err, errClusterNotReady) {
		t.Errorf("other 500s must be fatal, got %v", err)
	}
}

func TestParseProofStatus(t *testing.T) {
	for _, tc := range []struct {
		data, status, reason string
	}{
		{`"in_progress"`, "in_progress", ""},
		{`"completed"`, "completed", ""},
		{`"canceled"`, "canceled", ""},
		{`{"failing":"worker died"}`, "failing", "worker died"},
		{`{"failed":"worker died"}`, "failed", "worker died"},
	} {
		status, reason, err := parseProofStatus(tc.data)
		if err != nil || status != tc.status || reason != tc.reason {
			t.Errorf("parseProofStatus(%s) = %q, %q, %v", tc.data, status, reason, err)
		}
	}
	if _, _, err := parseProofStatus(`not json`); err == nil {
		t.Error("expected an error for malformed status")
	}
}

// The stand-in proof does not verify, so the run ends in the verifier's
// rejection after submission, the status stream, and the download.
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
	_, err := dialFakeCoordinator(t, server.URL).Prove(t.Context(), fake.input,
		func(phase string) { phases = append(phases, phase) })
	requireUndecodableProof(t, err)
	if len(phases) != 1 || phases[0] != "in_progress" {
		t.Errorf("phases = %v", phases)
	}
	fake.mu.Lock()
	uploaded, startedBody := fake.uploaded, fake.startedBody
	fake.mu.Unlock()
	if !bytes.Equal(uploaded, framedStdin(fake.input)) {
		t.Errorf("uploaded = %x", uploaded)
	}
	for _, required := range []string{`"program":{"name":"` + programName(testELF) + `","version":0}`, `"input_already_uploaded":false`} {
		if !strings.Contains(startedBody, required) {
			t.Errorf("start_proof body lacks %q: %s", required, startedBody)
		}
	}
}

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

	outcome, err := dialFakeCoordinator(t, server.URL).Prove(t.Context(), fake.input, func(string) {})
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

// A real manager answers /readyz on worker registration alone, so it reports
// itself ready while it refuses work. The measurement therefore discounts the
// refused attempts rather than trusting the gate to have covered them.
func TestDrainStaysOutOfTheMeasurement(t *testing.T) {
	for _, tc := range []struct {
		name               string
		readyzReportsDrain bool
	}{
		{name: "readyz reports the drain", readyzReportsDrain: true},
		{name: "readyz reports ready throughout", readyzReportsDrain: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCoordinator{
				t:                  t,
				proof:              readFixture(t, proofFixture),
				streams:            [][]string{{`"completed"`}},
				drain:              true,
				readyzReportsDrain: tc.readyzReportsDrain,
			}
			server := fake.server()
			defer server.Close()
			client := dialFakeCoordinator(t, server.URL)

			gateStarted := time.Now()
			if err := client.WaitReady(t.Context()); err != nil {
				t.Fatal(err)
			}
			gated := time.Since(gateStarted)
			started := time.Now()
			outcome, err := client.Prove(t.Context(), []byte("input"), func(string) {})
			if err != nil {
				t.Fatal(err)
			}
			measured := time.Since(started) - outcome.SubmitWait
			if measured > time.Second {
				t.Errorf("measured = %s, want the drain out of the measurement", measured)
			}
			// Each case has to reach the drain by its own route, or it passes
			// for want of a drain rather than for want of the accounting.
			if tc.readyzReportsDrain {
				if gated < time.Second {
					t.Errorf("gate = %s, want it to have seen the drain", gated)
				}
			} else if outcome.SubmitWait < submitRetryInterval {
				t.Errorf("submit wait = %s, want the refused attempt counted", outcome.SubmitWait)
			}
		})
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

	_, err := dialFakeCoordinator(t, server.URL).Prove(t.Context(), []byte("input"), func(string) {})
	requireUndecodableProof(t, err)
	fake.mu.Lock()
	subscriptions := fake.subscriptions
	fake.mu.Unlock()
	if subscriptions < 2 {
		t.Errorf("subscriptions = %d, want a reconnect", subscriptions)
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
	_, err := dialFakeCoordinator(t, server.URL).Prove(t.Context(), []byte("input"),
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

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err := dialFakeCoordinator(t, server.URL).Prove(ctx, []byte("input"), func(string) {})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want a timeout", err)
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
	var starts atomic.Int32
	mux.HandleFunc("POST /start_proof", func(w http.ResponseWriter, _ *http.Request) {
		starts.Add(1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"program_not_in_loadout","message":"Program x@v0 is not in the current loadout"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := dialFakeCoordinator(t, server.URL).Prove(t.Context(), []byte("input"), func(string) {})
	if err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("err = %v", err)
	}
	if starts.Load() != 1 {
		t.Errorf("starts = %d, a fatal rejection must not retry", starts.Load())
	}
}

func TestDial(t *testing.T) {
	server := (&fakeCoordinator{t: t}).server()
	defer server.Close()

	client, err := Dial(t.Context(), server.URL, testELF, programVerifyingKeyFixture(t))
	if err != nil {
		t.Fatalf("known program = %v", err)
	}
	if client.ProgramName != programName(testELF) {
		t.Errorf("ProgramName = %q", client.ProgramName)
	}
	if err := client.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if _, err := Dial(t.Context(), server.URL, []byte("other"), programVerifyingKeyFixture(t)); err == nil || !strings.Contains(err.Error(), "loadout") {
		t.Errorf("unknown program = %v", err)
	}
	if _, err := Dial(t.Context(), server.URL, testELF, []byte("baseline")); !errors.Is(err, ereverifier.ErrDecodeProgramVK) {
		t.Errorf("malformed verifying key = %v", err)
	}
}

func TestDialRejectsClusterVerifyingKeyMismatch(t *testing.T) {
	served := readFixture(t, otherBaselineFixture)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(served)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	expected := programVerifyingKeyFixture(t)
	_, err := Dial(t.Context(), server.URL, testELF, expected)
	if err == nil {
		t.Fatal("Dial() = nil, want a rejected cluster verifying key")
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

	if err := dialFakeCoordinator(t, server.URL).WaitReady(t.Context()); err != nil {
		t.Errorf("WaitReady = %v", err)
	}
}

// A wait cut short would leave the rest of a cluster's recovery inside the
// next proof's measurement, so only the caller ends it. The coordinator's own
// reason travels with the error.
func TestWaitReadyWaitsOnTheContext(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vk/{name}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(programVerifyingKeyFixture(t))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"ready":false,"message":"No Edge workers have registered yet"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := dialFakeCoordinator(t, server.URL).WaitReady(ctx)
	if err == nil {
		t.Fatal("WaitReady() = nil, want the cancelled context to end the wait")
	}
	if !strings.Contains(err.Error(), "No Edge workers have registered yet") {
		t.Errorf("err = %v, want the coordinator's reason", err)
	}
}

func TestVerifyProof(t *testing.T) {
	verifier := boundToFixture(t, programVerifyingKeyFixture(t))

	publicValues, err := verifyProof(verifier, readFixture(t, proofFixture))
	if err != nil {
		t.Fatalf("verifyProof() = %v, want a verified proof", err)
	}
	if want := readFixture(t, publicValuesFixture); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

func TestVerifyProofRejects(t *testing.T) {
	tampered := readFixture(t, proofFixture)
	tampered[len(tampered)/2] ^= 1
	cases := []struct {
		name     string
		baseline string
		proof    []byte
		wantErrs []error
		wantText string
	}{
		{
			name:     "another program",
			baseline: otherBaselineFixture,
			proof:    readFixture(t, proofFixture),
			wantErrs: []error{ereverifier.ErrVerify},
		},
		{
			name:     "tampered proof",
			baseline: baselineFixture,
			proof:    tampered,
			wantErrs: []error{ereverifier.ErrVerify, ereverifier.ErrDecodeProof},
		},
		{
			name:     "undecodable proof",
			baseline: baselineFixture,
			proof:    unverifiableProof,
			wantErrs: []error{ereverifier.ErrDecodeProof},
			wantText: "decoding the proof envelope",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verifier := boundToFixture(t, readFixture(t, tc.baseline))
			_, err := verifyProof(verifier, tc.proof)
			if err == nil {
				t.Fatal("verifyProof() = nil, want a rejection")
			}
			matched := false
			for _, want := range tc.wantErrs {
				matched = matched || errors.Is(err, want)
			}
			if !matched || !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("err = %v, want one of %v", err, tc.wantErrs)
			}
		})
	}
}
