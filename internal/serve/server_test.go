package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/han0110/provoor/internal/cluster"
)

// fakeProver answers with fixed public values or a fixed error. Each
// readiness wait costs readyDelay and each proof spends submitWait before
// reporting it, the shapes a cluster takes when it is still draining an
// earlier proof or refusing submissions.
type fakeProver struct {
	publicValues []byte
	err          error
	phases       []string
	pipeline     *cluster.Pipeline
	readyWaits   int
	readyDelay   time.Duration
	submitWait   time.Duration
}

func (p *fakeProver) WaitReady(context.Context) error {
	p.readyWaits++
	time.Sleep(p.readyDelay)
	return nil
}

func (p *fakeProver) Prove(_ context.Context, _ []byte, onPhase func(string)) (*cluster.ProveOutcome, error) {
	if p.err != nil {
		return nil, p.err
	}
	time.Sleep(p.submitWait)
	for _, phase := range p.phases {
		onPhase(phase)
	}
	return &cluster.ProveOutcome{
		PublicValues:       p.publicValues,
		ProofBytes:         316119,
		ClusterProvingTime: 4011 * time.Millisecond,
		SubmitWait:         p.submitWait,
		Pipeline:           p.pipeline,
	}, nil
}

// commitment returns fixed-size public values carrying output in the prefix.
func commitment(output []byte) []byte {
	publicValues := make([]byte, 256)
	copy(publicValues, output)
	return publicValues
}

func newServer(prover Prover, output *bytes.Buffer) *Server {
	return &Server{
		Prover:        prover,
		ClientVersion: "stateless-validator-fake-guest",
		Output:        output,
		Exit:          func(int) { panic("unexpected exit") },
	}
}

func post(t *testing.T, server *Server, body string) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	server.Handler().ServeHTTP(recorder, request)
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response %q: %v", recorder.Body.String(), err)
	}
	return decoded
}

func proveRequest(output []byte) string {
	payload := map[string]string{
		"blockHash":               "0xabc123",
		"blockNumber":             "0x1",
		"gasUsed":                 "0x3938700",
		"statelessInput":          "0x150102",
		"expectedStatelessOutput": "0x" + hex.EncodeToString(output),
	}
	encoded, _ := json.Marshal(payload)
	return `{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[` + string(encoded) + `],"id":7}`
}

// emittedMetric is one metric line, its pipeline left raw since the task rows
// are arrays on the wire.
type emittedMetric struct {
	metricLine
	Pipeline json.RawMessage `json:"pipeline"`
}

func lastMetric(t *testing.T, output *bytes.Buffer) emittedMetric {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	var metric emittedMetric
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &metric); err != nil {
		t.Fatalf("metric line %q: %v", lines[len(lines)-1], err)
	}
	return metric
}

func TestClientVersion(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	resp := post(t, server, `{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}`)
	if resp["result"] != "stateless-validator-fake-guest" {
		t.Errorf("result = %v", resp["result"])
	}
}

func TestWarmup(t *testing.T) {
	if err := newServer(&fakeProver{publicValues: commitment(warmupOutput)}, &bytes.Buffer{}).Warmup(t.Context()); err != nil {
		t.Errorf("Warmup = %v", err)
	}
	// A guest built for another input format commits to an error at once and
	// warms no worker at all.
	server := newServer(&fakeProver{publicValues: commitment([]byte{9})}, &bytes.Buffer{})
	if err := server.Warmup(t.Context()); err == nil || !strings.Contains(err.Error(), "does not compute it") {
		t.Errorf("Warmup = %v, want the output mismatch reported", err)
	}
	server.Prover = &fakeProver{err: errors.New("down")}
	if err := server.Warmup(t.Context()); err == nil || !strings.Contains(err.Error(), "warming up the prover: down") {
		t.Errorf("Warmup = %v, want the prover error wrapped", err)
	}
}

func TestProveValid(t *testing.T) {
	expected := []byte{1, 2, 3, 4}
	output := &bytes.Buffer{}
	pipeline := &cluster.Pipeline{
		SchemaVersion: 1,
		Kinds:         []cluster.TaskKind{{Name: "segment", Label: "Segment", Phase: cluster.PhaseSegment}},
		Breakdown:     []string{"Trace Gen"},
		Workers:       []cluster.Worker{{Name: "worker_0", Node: "node1"}},
		Tasks:         []cluster.Task{{ID: "24", StartMs: 1015, DurationMs: 1100, Breakdown: [][2]int64{{0, 40}}}},
	}
	server := newServer(&fakeProver{
		publicValues: commitment(expected),
		phases:       []string{"queued", "prove", "prove", "recurse"},
		pipeline:     pipeline,
	}, output)

	resp := post(t, server, proveRequest(expected))
	result := resp["result"].(map[string]any)
	if len(result) != 1 || result["status"] != "VALID" {
		t.Errorf("a passing proof answers the status alone, got %v", result)
	}
	metric := lastMetric(t, output)
	if metric.Block.Hash != "0xabc123" || !metric.OutputMatched || metric.StatelessInputSize != 3 {
		t.Errorf("metric = %+v", metric)
	}
	if metric.ProofSize != 316119 || metric.ClusterReportedProvingTimeMs != 4011 {
		t.Errorf("proving metric = %v/%v", metric.ProofSize, metric.ClusterReportedProvingTimeMs)
	}
	if metric.Block.Number != 1 || metric.Block.GasUsed != 60000000 {
		t.Errorf("block metric = %+v", metric.Block)
	}
	if metric.Throughput.MGasPerSec <= 0 || metric.Timing.TotalMs != metric.ProvingTimeMs {
		t.Errorf("throughput metric = %+v timing = %+v", metric.Throughput, metric.Timing)
	}
	if want, _ := json.Marshal(pipeline); !bytes.Equal(metric.Pipeline, want) {
		t.Errorf("pipeline = %s, want %s", metric.Pipeline, want)
	}
	// A phase repeated by a poll prints once.
	if got := strings.Count(output.String(), "phase prove"); got != 1 {
		t.Errorf("phase prove printed %d times, want once, output %q", got, output.String())
	}
	if !strings.Contains(output.String(), "phase recurse") {
		t.Errorf("expected phase lines in output, got %q", output.String())
	}
}

func TestProveMismatch(t *testing.T) {
	output := &bytes.Buffer{}
	server := newServer(&fakeProver{publicValues: commitment([]byte{9, 9, 9})}, output)

	resp := post(t, server, proveRequest([]byte{1, 2, 3}))
	result := resp["result"].(map[string]any)
	if result["status"] != "INVALID" || result["validationError"] != "stateless output mismatch" {
		t.Errorf("result = %v", result)
	}
	if result["statelessOutput"] != "0x"+hex.EncodeToString(commitment([]byte{9, 9, 9})) {
		t.Errorf("a mismatch echoes the committed output, got %v", result["statelessOutput"])
	}
	if lastMetric(t, output).OutputMatched {
		t.Error("metric must record the mismatch")
	}
}

func TestProveClusterError(t *testing.T) {
	server := newServer(&fakeProver{err: errors.New("cluster unavailable: down")}, &bytes.Buffer{})
	resp := post(t, server, proveRequest([]byte{1}))
	if resp["result"] != nil {
		t.Errorf("result = %v", resp["result"])
	}
	rpcErr := resp["error"].(map[string]any)
	if rpcErr["code"] != float64(-32000) || !strings.Contains(rpcErr["message"].(string), "cluster unavailable") {
		t.Errorf("error = %v", rpcErr)
	}
}

func TestProveClusterErrorFailsRun(t *testing.T) {
	exited := 0
	server := newServer(&fakeProver{err: errors.New("down")}, &bytes.Buffer{})
	server.FailRunOnClusterError = true
	server.Exit = func(code int) { exited = code; panic("exit") }

	defer func() {
		if recover() == nil || exited != 1 {
			t.Errorf("expected exit 1, got %d", exited)
		}
	}()
	post(t, server, proveRequest([]byte{1}))
}

// TestProveClientDisconnectDoesNotFailRun pins that a client going away is
// never attributed to the cluster, whatever error the backend returns.
func TestProveClientDisconnectDoesNotFailRun(t *testing.T) {
	server := newServer(&fakeProver{err: errors.New("prove job 7 timed out")}, &bytes.Buffer{})
	server.FailRunOnClusterError = true
	server.Exit = func(int) { t.Error("a disconnected client must not fail the run") }

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest("POST", "/", strings.NewReader(proveRequest([]byte{1}))).WithContext(ctx)
	server.Handler().ServeHTTP(httptest.NewRecorder(), request)
}

func TestMethodNotFound(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	for _, method := range []string{"eth_getBlockByNumber", "engine_newPayloadV5"} {
		resp := post(t, server, `{"jsonrpc":"2.0","method":"`+method+`","params":[],"id":1}`)
		rpcErr, ok := resp["error"].(map[string]any)
		if !ok || rpcErr["code"] != float64(-32601) {
			t.Errorf("%s: error = %v", method, resp["error"])
		}
	}
}

func TestParseError(t *testing.T) {
	resp := post(t, newServer(&fakeProver{}, &bytes.Buffer{}), `{`)
	rpcErr, ok := resp["error"].(map[string]any)
	if !ok || rpcErr["code"] != float64(-32700) {
		t.Errorf("error = %v", resp["error"])
	}
}

func TestProveRejectsMalformedParams(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	cases := []string{
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[],"id":1}`,
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[{"statelessInput":"noprefix"}],"id":1}`,
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[{"statelessInput":"0xzz"}],"id":1}`,
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[{"statelessInput":"0x01","expectedStatelessOutput":"01"}],"id":1}`,
	}
	for _, body := range cases {
		resp := post(t, server, body)
		rpcErr, ok := resp["error"].(map[string]any)
		if !ok || rpcErr["code"] != float64(-32602) {
			t.Errorf("body %s: error = %v", body, resp["error"])
		}
	}
}

// TestWaitReadyPrecedesEveryProof pins the readiness gate on every request,
// so a cluster draining an earlier proof never charges its recovery to the
// next block.
func TestWaitReadyPrecedesEveryProof(t *testing.T) {
	expected := []byte{1, 2, 3, 4}
	passing := &fakeProver{publicValues: commitment(expected)}
	server := newServer(passing, &bytes.Buffer{})
	post(t, server, proveRequest(expected))
	post(t, server, proveRequest(expected))
	if passing.readyWaits != 2 {
		t.Errorf("readyWaits = %d, want one wait before each proof", passing.readyWaits)
	}
	failing := &fakeProver{err: errors.New("down")}
	server = newServer(failing, &bytes.Buffer{})
	post(t, server, proveRequest([]byte{1}))
	if failing.readyWaits != 1 {
		t.Errorf("readyWaits = %d, want the first proof gated too", failing.readyWaits)
	}
}

// TestNothingBeforeAdmissionIsMeasured pins the clock against both ways a
// cluster keeps a block waiting, the readiness gate before the clock and the
// refused submissions subtracted from the total.
func TestNothingBeforeAdmissionIsMeasured(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prover *fakeProver
	}{
		{"the cluster is not ready yet", &fakeProver{readyDelay: 80 * time.Millisecond}},
		{"the cluster reports ready and refuses", &fakeProver{submitWait: 80 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := []byte{1, 2, 3, 4}
			tc.prover.publicValues = commitment(expected)
			output := &bytes.Buffer{}
			post(t, newServer(tc.prover, output), proveRequest(expected))
			metric := lastMetric(t, output)
			if metric.ProvingTimeMs >= 40 {
				t.Errorf("provingTimeMs = %d, want the 80ms wait left out", metric.ProvingTimeMs)
			}
			if metric.Timing.TotalMs != metric.ProvingTimeMs {
				t.Errorf("timing = %+v, want total_ms to follow provingTimeMs", metric.Timing)
			}
		})
	}
}

// TestWaitsPastASecondAreReported covers the two waits the measurement leaves
// out, so a block that took longer than its proof leaves a trace of where the
// time went.
func TestWaitsPastASecondAreReported(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prover *fakeProver
		want   string
	}{
		{"readiness", &fakeProver{readyDelay: 1100 * time.Millisecond}, "waited 1s for the cluster before proving 0xabc123"},
		{"admission", &fakeProver{submitWait: 1100 * time.Millisecond}, "waited 1s for the cluster to admit 0xabc123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := []byte{1, 2, 3, 4}
			tc.prover.publicValues = commitment(expected)
			output := &bytes.Buffer{}
			post(t, newServer(tc.prover, output), proveRequest(expected))
			if !strings.Contains(output.String(), tc.want) {
				t.Errorf("output %q lacks %q", output.String(), tc.want)
			}
		})
	}
}

func TestOutputMatched(t *testing.T) {
	publicValues := commitment([]byte{1, 2, 3})
	if !outputMatched(publicValues, []byte{1, 2, 3}) || !outputMatched(publicValues, publicValues) {
		t.Error("expected a prefix with a zero tail and full equality to match")
	}
	if outputMatched(publicValues, []byte{1, 2}) || outputMatched([]byte{1}, []byte{1, 2}) {
		t.Error("expected a nonzero tail and an expected output longer than the commitment to mismatch")
	}
	dirty := commitment([]byte{1, 2, 3})
	dirty[200] = 1
	if outputMatched(dirty, []byte{1, 2, 3}) {
		t.Error("expected mismatch on a nonzero trailing byte")
	}
}
