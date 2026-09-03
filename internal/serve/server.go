// Package serve forwards benchmark requests to a proving cluster as a
// JSON-RPC server shaped like an execution client. One request proves one
// stateless payload and the response mirrors an Engine API payload status.
package serve

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/han0110/provoor/internal/cluster"
)

// warmupInput is the stateless input of the 60M gas PUSH28 block of the EEST
// tests-zkevm-benchmark@v0.8.2 release. It splits into about 230 segments, so
// every worker of a cluster up to that size receives a shard. Each one pays
// its one-time costs before the first measured proof.
//
//go:embed warmup-input.bin
var warmupInput []byte

// warmupOutput is the stateless output the warmup block commits to. A guest
// that commits to anything else did not run the block and warmed nothing.
var warmupOutput = mustHex("c26fa548f160616d5ad455673ec0775fba5c1453196f8c8825af28391fa53bcb0101000000000000000115")

// Prover proves one stateless payload per call.
type Prover interface {
	// WaitReady blocks until every worker is back, or returns at once on a
	// cluster whose coordinator reports nothing a client can trust. It runs
	// outside the measured interval.
	WaitReady(ctx context.Context) error
	// Prove proves the payload in input, bounded by the context deadline, and
	// reports every job phase it observes through onPhase.
	Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*cluster.ProveOutcome, error)
}

// Server answers the JSON-RPC methods a benchmark run needs. A mutex
// serialises proofs, so two callers cannot put two jobs on one cluster.
type Server struct {
	Prover Prover
	// ClientVersion answers web3_clientVersion, the guest ELF name.
	ClientVersion string
	// ProveTimeout bounds one proof, cluster.DefaultProveTimeout when unset.
	ProveTimeout time.Duration
	// FailRunOnClusterError exits the process on a cluster error instead of
	// failing the test.
	FailRunOnClusterError bool
	// Output receives the phase and metric lines.
	Output io.Writer
	// Exit terminates the process, os.Exit in production.
	Exit func(code int)

	mu sync.Mutex
}

type request struct {
	JSONRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
	ID      json.RawMessage   `json:"id"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// provePayload is params[0] of engine_proveStatelessValidator.
type provePayload struct {
	BlockHash               string `json:"blockHash"`
	BlockNumber             string `json:"blockNumber"`
	GasUsed                 string `json:"gasUsed"`
	StatelessInput          string `json:"statelessInput"`
	ExpectedStatelessOutput string `json:"expectedStatelessOutput"`
}

// payloadStatus mirrors an Engine API payload status. The committed output
// is echoed only on a mismatch, where it is worth diffing against the
// fixture.
type payloadStatus struct {
	Status          string `json:"status"`
	ValidationError string `json:"validationError,omitempty"`
	StatelessOutput string `json:"statelessOutput,omitempty"`
}

// metricLine is the per-test JSON line the benchmark collector reads. The
// block hash nests under block.hash, and block, timing, and throughput reuse
// the block-log field names the UI dashboards read.
type metricLine struct {
	Block                        metricBlock      `json:"block"`
	Timing                       metricTiming     `json:"timing"`
	Throughput                   metricThroughput `json:"throughput"`
	StatelessInputSize           int              `json:"statelessInputSize"`
	ProvingTimeMs                int64            `json:"provingTimeMs"`
	ClusterReportedProvingTimeMs int64            `json:"clusterReportedProvingTimeMs"`
	ProofSize                    int              `json:"proofSize"`
	OutputMatched                bool             `json:"outputMatched"`
}

type metricBlock struct {
	Number  uint64 `json:"number"`
	Hash    string `json:"hash"`
	GasUsed uint64 `json:"gas_used"`
}

type metricTiming struct {
	TotalMs int64 `json:"total_ms"`
}

type metricThroughput struct {
	MGasPerSec float64 `json:"mgas_per_sec"`
}

// Warmup proves the warmup block once, so a cold cluster's one-time costs
// land before the first measured proof, and checks the committed output. A
// guest built for another input format commits to an error at once and
// warms nothing.
func (s *Server) Warmup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.proveTimeout())
	defer cancel()
	outcome, err := s.Prover.Prove(ctx, warmupInput, func(string) {})
	if err != nil {
		return fmt.Errorf("warming up the prover: %w", err)
	}
	if !outputMatched(outcome.PublicValues, warmupOutput) {
		return fmt.Errorf("the warmup block committed 0x%x, want 0x%x, so this guest does not compute it", outcome.PublicValues, warmupOutput)
	}
	return nil
}

func (s *Server) proveTimeout() time.Duration {
	if s.ProveTimeout <= 0 {
		return cluster.DefaultProveTimeout
	}
	return s.ProveTimeout
}

// Handler returns the HTTP handler answering JSON-RPC requests.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req request
		if err := json.Unmarshal(body, &req); err != nil {
			writeResponse(w, response{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: "parse error"}})
			return
		}
		resp := response{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "web3_clientVersion":
			resp.Result = s.ClientVersion
		case "engine_proveStatelessValidator":
			resp.Result, resp.Error = s.prove(r.Context(), req.Params)
		default:
			resp.Error = &rpcError{Code: -32601, Message: fmt.Sprintf("the method %s does not exist", req.Method)}
		}
		writeResponse(w, resp)
	})
}

func writeResponse(w http.ResponseWriter, resp response) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) prove(requestCtx context.Context, params []json.RawMessage) (any, *rpcError) {
	if len(params) == 0 {
		return nil, &rpcError{Code: -32602, Message: "params[0] is required"}
	}
	var payload provePayload
	if err := json.Unmarshal(params[0], &payload); err != nil {
		return nil, &rpcError{Code: -32602, Message: "params[0]: " + err.Error()}
	}
	input, err := decodeHex(payload.StatelessInput)
	if err != nil {
		return nil, &rpcError{Code: -32602, Message: "statelessInput: " + err.Error()}
	}
	expected, err := decodeHex(payload.ExpectedStatelessOutput)
	if err != nil {
		return nil, &rpcError{Code: -32602, Message: "expectedStatelessOutput: " + err.Error()}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The readiness wait runs on the request context, outside the proof's
	// budget and its measurement. A cluster draining an earlier proof reports
	// itself ready and refuses the job anyway, which the outcome's submit wait
	// accounts for.
	readyStarted := time.Now()
	if err := s.Prover.WaitReady(requestCtx); err != nil {
		s.printf("cluster still not ready, proving %s anyway: %v", payload.BlockHash, err)
	} else if waited := time.Since(readyStarted); waited > time.Second {
		s.printf("waited %s for the cluster before proving %s", waited.Round(time.Second), payload.BlockHash)
	}

	ctx, cancel := context.WithTimeout(requestCtx, s.proveTimeout())
	defer cancel()
	s.printf("proving %s (%d input bytes)", payload.BlockHash, len(input))
	started := time.Now()
	lastPhase := ""
	outcome, err := s.Prover.Prove(ctx, input, func(phase string) {
		if phase != lastPhase {
			lastPhase = phase
			s.printf("proving %s phase %s", payload.BlockHash, phase)
		}
	})
	if err != nil {
		s.printf("proving %s failed: %v", payload.BlockHash, err)
		// A client that went away says nothing about the cluster's health.
		if s.FailRunOnClusterError && requestCtx.Err() == nil {
			s.Exit(1)
		}
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	provingTime := time.Since(started) - outcome.SubmitWait
	if outcome.SubmitWait > time.Second {
		s.printf("waited %s for the cluster to admit %s", outcome.SubmitWait.Round(time.Second), payload.BlockHash)
	}

	matched := outputMatched(outcome.PublicValues, expected)
	gasUsed := parseQuantity(payload.GasUsed)
	var mgasPerSec float64
	if provingTime > 0 {
		mgasPerSec = float64(gasUsed) / 1e6 / provingTime.Seconds()
	}
	s.emitMetric(metricLine{
		Block:                        metricBlock{Number: parseQuantity(payload.BlockNumber), Hash: payload.BlockHash, GasUsed: gasUsed},
		Timing:                       metricTiming{TotalMs: provingTime.Milliseconds()},
		Throughput:                   metricThroughput{MGasPerSec: mgasPerSec},
		StatelessInputSize:           len(input),
		ProvingTimeMs:                provingTime.Milliseconds(),
		ClusterReportedProvingTimeMs: outcome.ClusterProvingTime.Milliseconds(),
		ProofSize:                    outcome.ProofBytes,
		OutputMatched:                matched,
	})

	status := payloadStatus{Status: "VALID"}
	if !matched {
		status.Status = "INVALID"
		status.ValidationError = "stateless output mismatch"
		status.StatelessOutput = "0x" + hex.EncodeToString(outcome.PublicValues)
	}
	return status, nil
}

// outputMatched reports whether the fixed-size commitment carries the
// expected output as its prefix with an all-zero tail.
func outputMatched(publicValues, expected []byte) bool {
	if len(expected) > len(publicValues) || !bytes.Equal(publicValues[:len(expected)], expected) {
		return false
	}
	for _, trailing := range publicValues[len(expected):] {
		if trailing != 0 {
			return false
		}
	}
	return true
}

// parseQuantity reads a hex or decimal quantity, zero when absent or
// malformed, since the metric fields it feeds are best effort.
func parseQuantity(value string) uint64 {
	if after, ok := strings.CutPrefix(value, "0x"); ok {
		parsed, _ := strconv.ParseUint(after, 16, 64)
		return parsed
	}
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

func decodeHex(value string) ([]byte, error) {
	trimmed, ok := strings.CutPrefix(value, "0x")
	if !ok {
		return nil, fmt.Errorf("missing 0x prefix")
	}
	return hex.DecodeString(trimmed)
}

func mustHex(text string) []byte {
	decoded, err := hex.DecodeString(text)
	if err != nil {
		panic(err)
	}
	return decoded
}

func (s *Server) printf(format string, args ...any) {
	fmt.Fprintf(s.Output, format+"\n", args...)
}

func (s *Server) emitMetric(metric metricLine) {
	encoded, _ := json.Marshal(metric)
	s.printf("%s", encoded)
}
