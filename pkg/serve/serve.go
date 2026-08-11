// Package serve forwards benchmark requests to a proving backend as a
// JSON-RPC server shaped like an execution client, so a benchmark harness
// drives proving through its unchanged request loop. One request proves one
// stateless payload, and the response mirrors an Engine API payload status.
package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultProveTimeout bounds one proof. Shorter timeouts misreport slow
// proofs as failures, so the default sits well above routine proving times.
const DefaultProveTimeout = 10 * time.Minute

// Prover proves one stateless payload per call. Implementations are supplied
// by the zkVM backend packages.
type Prover interface {
	// ClientVersion is the guest ELF name, identifying the guest and its
	// zkVM SDK version for run records.
	ClientVersion() string
	// Warmup proves a small fixed input, so a cold prover's one-time costs
	// land before the first measured proof.
	Warmup(ctx context.Context) error
	// Prove proves the payload carried in input, bounded by the context
	// deadline, reporting job phase transitions through onPhase.
	Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*ProveOutcome, error)
}

// ProveOutcome carries what one completed proof reports.
type ProveOutcome struct {
	// PublicValues is the fixed-size commitment the guest produced.
	PublicValues []byte
	// ProofBytes is the size of the returned proof envelope.
	ProofBytes int
	// ClusterProvingTime is the backend's self-reported proving duration.
	ClusterProvingTime time.Duration
}

// Server answers the JSON-RPC methods a benchmark run needs. A mutex
// serialises proofs, so concurrent callers cannot put two jobs on one
// cluster and corrupt both timings.
type Server struct {
	// Prover proves payloads.
	Prover Prover
	// ProveTimeout bounds one proof, DefaultProveTimeout when zero.
	ProveTimeout time.Duration
	// FailRunOnClusterError exits the process on a cluster error instead of
	// failing the test, mapping an unrecoverable fault onto container death.
	FailRunOnClusterError bool
	// Output receives the phase and metric lines, one per line.
	Output io.Writer
	// Exit terminates the process, os.Exit in production.
	Exit func(code int)

	mu       sync.Mutex
	outputMu sync.Mutex
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

// payloadStatus mirrors an Engine API payload status, so the harness decides
// pass and fail without new logic. Proving measurements and the subject under
// test travel on the metric lines instead, which is where the collector reads
// them from. The committed output is echoed only on a mismatch, where it is
// the one thing worth diffing against the fixture.
type payloadStatus struct {
	Status          string `json:"status"`
	ValidationError string `json:"validationError,omitempty"`
	StatelessOutput string `json:"statelessOutput,omitempty"`
}

// metricLine is the per-test JSON object written to Output. The block hash
// nests under block.hash, the shape every block-log payload shares, so the
// collector matches the line to its test. Block, timing, and throughput
// reuse the standard block-log field names the UI dashboards read. Like an
// execution client's own block logs, the line carries measurements only, and
// the subject under test is named once per run by the harness configuration.
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

// parseQuantity reads a hex or decimal quantity string, zero when absent or
// malformed, since the metric fields it feeds are best effort.
func parseQuantity(value string) uint64 {
	if after, ok := strings.CutPrefix(value, "0x"); ok {
		parsed, _ := strconv.ParseUint(after, 16, 64)
		return parsed
	}
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
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
			writeResponse(w, response{JSONRPC: "2.0", ID: nil, Error: &rpcError{Code: -32700, Message: "parse error"}})
			return
		}

		resp := response{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "web3_clientVersion":
			resp.Result = s.Prover.ClientVersion()
		case "engine_proveStatelessValidator":
			result, rpcErr := s.prove(r.Context(), req.Params)
			resp.Result, resp.Error = result, rpcErr
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

func (s *Server) prove(ctx context.Context, params []json.RawMessage) (any, *rpcError) {
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

	timeout := s.ProveTimeout
	if timeout == 0 {
		timeout = DefaultProveTimeout
	}
	// The request context reports whether the benchmark client is still
	// waiting, independent of how a backend wraps its own errors.
	requestCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	s.printf("proving %s (%d input bytes)", payload.BlockHash, len(input))
	started := time.Now()
	outcome, err := s.Prover.Prove(ctx, input, func(phase string) {
		s.printf("proving %s phase %s", payload.BlockHash, phase)
	})
	if err != nil {
		s.printf("proving %s failed: %v", payload.BlockHash, err)
		// A client that went away says nothing about the cluster's health,
		// while a proof exceeding its own budget is the fault this exits on.
		if s.FailRunOnClusterError && requestCtx.Err() == nil {
			s.Exit(1)
		}
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	provingTime := time.Since(started)

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

// outputMatched reports whether the committed public values carry the
// expected output. The commitment is fixed size and the output shorter, so a
// match requires prefix equality and all-zero trailing bytes.
func outputMatched(publicValues, expected []byte) bool {
	if len(expected) > len(publicValues) {
		return false
	}
	if !bytes.Equal(publicValues[:len(expected)], expected) {
		return false
	}
	for _, trailing := range publicValues[len(expected):] {
		if trailing != 0 {
			return false
		}
	}
	return true
}

func decodeHex(value string) ([]byte, error) {
	trimmed, ok := strings.CutPrefix(value, "0x")
	if !ok {
		return nil, fmt.Errorf("missing 0x prefix")
	}
	return hex.DecodeString(trimmed)
}

func (s *Server) printf(format string, args ...any) {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	fmt.Fprintf(s.Output, format+"\n", args...)
}

func (s *Server) emitMetric(metric metricLine) {
	encoded, _ := json.Marshal(metric)
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	fmt.Fprintf(s.Output, "%s\n", encoded)
}
