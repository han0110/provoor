package openvm

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"slices"
	"time"

	"github.com/han0110/provoor/internal/cluster"
)

// Indices into pipelineKinds.
const (
	kindExecution = iota
	kindFastForward
	kindSegment
	kindLeaf
	kindInternal
	kindWrap
)

var pipelineKinds = []cluster.TaskKind{
	{Name: "execution", Label: "Metered Execution", Legend: "Metered Execution", Row: "Segment", Phase: cluster.PhaseExecution},
	{Name: "fastfwd", Label: "Fast Forward", Legend: "Fast Forward", Row: "Segment", Phase: cluster.PhaseExecution},
	{Name: "segment", Label: "Segment", Legend: "Segment", Row: "Segment", Phase: cluster.PhaseBase},
	{Name: "leaf", Label: "Leaf Aggregation", Legend: "Leaf Aggregation", Row: "Leaf Aggregation", Phase: cluster.PhaseRecursion},
	{Name: "internal", Label: "Internal Aggregation", Legend: "Internal Aggregation", Row: "Internal Aggregation", Phase: cluster.PhaseRecursion},
	{Name: "wrap", Label: "Wrap", Legend: "Wrap", Row: "Internal Aggregation", Phase: cluster.PhaseWrap},
}

// pipelineBreakdown pairs each breakdown label with the sub metric carrying
// it, in label order.
var pipelineBreakdown = []struct{ label, subMetric string }{
	{"Execute Preflight", "execute_preflight_time_ms"},
	{"Postflight", "postflight_time_ms"},
	{"Trace Gen", "trace_gen_time_ms"},
	{"Commit", "prover.main_trace_commit_time_ms"},
	{"LogUp GKR", "prover.rap_constraints.logup_gkr_time_ms"},
	{"Sumcheck Univariate Skip", "prover.rap_constraints.round0_time_ms"},
	{"Sumcheck Multilinear Rounds", "prover.rap_constraints.mle_rounds_time_ms"},
	{"Open", "prover.openings_time_ms"},
}

// breakdownLabels are the labels of pipelineBreakdown, in the same order.
var breakdownLabels = func() []string {
	labels := make([]string, len(pipelineBreakdown))
	for index, entry := range pipelineBreakdown {
		labels[index] = entry.label
	}
	return labels
}()

// pipelineView is the coordinator's view of one proof's task results, without
// the proof bytes.
type pipelineView struct {
	ProofStartTime time.Time    `json:"proof_start_time"`
	AppProofs      []taskTiming `json:"app_proofs"`
	LeafProofs     []taskTiming `json:"leaf_proofs"`
	InternalProofs []taskTiming `json:"internal_proofs"`
}

// taskTiming is one task result, stamped by the manager with the worker that
// produced it and the clock at receipt.
type taskTiming struct {
	WorkerID          int                `json:"worker_id"`
	CompletedAtMs     int64              `json:"completed_at_ms"`
	SegmentStart      int                `json:"segment_start"`
	SegmentEnd        int                `json:"segment_end"`
	LayerIndex        int                `json:"layer_idx"`
	QueueWaitMs       int64              `json:"queue_wait_ms"`
	MeteredTimeMs     int64              `json:"metered_time_ms"`
	ProveTimeMs       int64              `json:"prove_time_ms"`
	FastForwardTimeMs int64              `json:"fastfwd_time_ms"`
	StarkProveTimeMs  int64              `json:"stark_prove_time_ms"`
	CompressionTimeMs int64              `json:"compression_time_ms"`
	SubMetrics        map[string]float64 `json:"sub_metrics"`
	WrapSubMetrics    map[string]float64 `json:"wrap_sub_metrics"`
}

type workerList struct {
	Workers []workerRegistration `json:"workers"`
}

// workerRegistration is one entry of the worker list, a pair of the worker id
// and the registration it holds.
type workerRegistration struct {
	WorkerID  int
	WorkerURL string
}

func (w *workerRegistration) UnmarshalJSON(data []byte) error {
	var pair [2]json.RawMessage
	if err := json.Unmarshal(data, &pair); err != nil {
		return err
	}
	var registered struct {
		WorkerURL string `json:"worker_url"`
	}
	if err := json.Unmarshal(pair[1], &registered); err != nil {
		return err
	}
	w.WorkerURL = registered.WorkerURL
	return json.Unmarshal(pair[0], &w.WorkerID)
}

// proofPipeline reads the settled proof's task view and the worker list, and
// maps them onto the timeline shape every zkVM shares. The manager evicts
// settled proofs after a few minutes, which this read directly after settling
// stays well inside.
func (c *Client) proofPipeline(ctx context.Context, proofUUID string) (*cluster.Pipeline, error) {
	var view pipelineView
	if err := c.fetchJSON(ctx, "/proof_pipeline/"+proofUUID, &view); err != nil {
		return nil, fmt.Errorf("reading proof %s pipeline: %w", proofUUID, err)
	}
	var workers workerList
	if err := c.fetchJSON(ctx, "/workers", &workers); err != nil {
		return nil, fmt.Errorf("reading the worker list: %w", err)
	}
	return mapPipeline(&view, workers.Workers)
}

// mapPipeline places every task result on the proof clock. The manager stamps
// each record on receipt, and the durations it reports set the record's tasks
// back from that stamp. A task reaching before the proof start is cut off at
// zero, and a task of no length is left out.
func mapPipeline(view *pipelineView, registrations []workerRegistration) (*cluster.Pipeline, error) {
	builder := cluster.NewPipelineBuilder(mapRegistrations(registrations))
	proofStartMs := view.ProofStartTime.UnixMilli()
	place := func(kind int, timing taskTiming, id string, endMs, durationMs int64, breakdown [][2]int64) int64 {
		return builder.Place(kind, timing.WorkerID, id, endMs, durationMs, breakdown)
	}

	for _, timing := range view.AppProofs {
		id := fmt.Sprintf("#%d", timing.SegmentStart)
		endMs := timing.CompletedAtMs - proofStartMs
		segmentStartMs := place(kindSegment, timing, id, endMs, timing.StarkProveTimeMs, mapBreakdown(timing.SubMetrics))
		place(kindFastForward, timing, id, segmentStartMs, timing.FastForwardTimeMs, nil)
		place(kindExecution, timing, id, endMs-timing.ProveTimeMs-timing.QueueWaitMs, timing.MeteredTimeMs, nil)
	}
	for _, timing := range view.LeafProofs {
		place(kindLeaf, timing, segmentRange(timing), timing.CompletedAtMs-proofStartMs, timing.ProveTimeMs, mapBreakdown(timing.SubMetrics))
	}
	for _, timing := range view.InternalProofs {
		id := fmt.Sprintf("L%d %s", timing.LayerIndex, segmentRange(timing))
		endMs := timing.CompletedAtMs - proofStartMs
		place(kindInternal, timing, id, endMs-timing.CompressionTimeMs, timing.ProveTimeMs, mapBreakdown(timing.SubMetrics))
		place(kindWrap, timing, id, endMs, timing.CompressionTimeMs, mapBreakdown(timing.WrapSubMetrics))
	}
	return builder.Pipeline(pipelineKinds, breakdownLabels)
}

// mapRegistrations orders the registered workers by worker id and reads the
// node of each worker off the URL the coordinator dials back.
func mapRegistrations(registrations []workerRegistration) []cluster.Registration[int] {
	byWorkerID := slices.SortedFunc(slices.Values(registrations), func(a, b workerRegistration) int {
		return cmp.Compare(a.WorkerID, b.WorkerID)
	})
	mapped := make([]cluster.Registration[int], len(byWorkerID))
	for index, registration := range byWorkerID {
		mapped[index] = cluster.Registration[int]{
			Key:  registration.WorkerID,
			Name: fmt.Sprintf("worker_%d", registration.WorkerID),
			Host: workerHost(registration.WorkerURL),
		}
	}
	return mapped
}

// workerHost is the host of a worker URL, the whole URL when it does not parse.
func workerHost(workerURL string) string {
	parsed, err := url.Parse(workerURL)
	if err != nil {
		return workerURL
	}
	return parsed.Hostname()
}

func segmentRange(timing taskTiming) string {
	return fmt.Sprintf("%d-%d", timing.SegmentStart, timing.SegmentEnd)
}

// mapBreakdown indexes the sub metrics the frontend labels, in label order
// and rounded to whole milliseconds. A sub metric the task lacks is left out.
func mapBreakdown(subMetrics map[string]float64) [][2]int64 {
	var breakdown [][2]int64
	for index, entry := range pipelineBreakdown {
		if value, reported := subMetrics[entry.subMetric]; reported {
			breakdown = append(breakdown, [2]int64{int64(index), int64(math.Round(value))})
		}
	}
	return breakdown
}
