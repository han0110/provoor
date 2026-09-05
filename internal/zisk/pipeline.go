package zisk

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/zisk/api"
)

// Indices into pipelineKinds.
const (
	kindExecute = iota
	kindPrecalc
	kindWitness
	kindRecompute
	kindRecursionWitness
	kindAggregationWitness
	kindChallenge
	kindCommit
	kindBasic
	kindCompressor
	kindRecursive1
	kindRecursive2
	kindVadcopFinal
	kindCompressed
)

// kindUnmapped marks a record kind the timeline does not draw.
const kindUnmapped = -1

// pipelineKinds names every bar of one worker. The legend groups every witness
// build, the contribution with its challenge, and the two finals under one
// entry each, so each group shares one phase. Every witness build takes the
// witness phase, the contribution and the basic proof take the base phase,
// and the recursive proofs take the recursion phase. An instance draws its
// contributions phase on one row and its proofs on a second row.
var pipelineKinds = []cluster.TaskKind{
	{Name: "execute", Label: "Execute", Legend: "Execute", Row: "Execute", Phase: cluster.PhaseExecution},
	{Name: "precalc", Label: "Pre Calculate Witness Generation", Legend: "Witness Generation", Row: "Pre Calculate Witness Generation", Phase: cluster.PhaseWitness},
	{Name: "witness", Label: "Witness Generation", Legend: "Witness Generation", Row: "Contribution", Phase: cluster.PhaseWitness},
	{Name: "recompute", Label: "Witness Recompute", Legend: "Witness Generation", Row: "Proof", Phase: cluster.PhaseWitness},
	{Name: "recursionwitness", Label: "Recursion Witness Generation", Legend: "Witness Generation", Row: "Proof", Phase: cluster.PhaseWitness},
	{Name: "aggregationwitness", Label: "Aggregation Witness Generation", Legend: "Witness Generation", Row: "Fold", Phase: cluster.PhaseWitness},
	{Name: "challenge", Label: "Calculate Internal Contributions", Legend: "Contribution", Row: "Calculate Internal Contributions", Phase: cluster.PhaseBase},
	{Name: "commit", Label: "Contribution", Legend: "Contribution", Row: "Contribution", Phase: cluster.PhaseBase},
	{Name: "basic", Label: "Basic Proof", Legend: "Basic Proof", Row: "Proof", Phase: cluster.PhaseBase},
	{Name: "compressor", Label: "Compressor Proof", Legend: "Compressor Proof", Row: "Proof", Phase: cluster.PhaseRecursion},
	{Name: "recursive1", Label: "Recursive1 Proof", Legend: "Recursive1 Proof", Row: "Proof", Phase: cluster.PhaseRecursion},
	{Name: "recursive2", Label: "Recursive2 Proof", Legend: "Recursive2 Proof", Row: "Fold", Phase: cluster.PhaseRecursion},
	{Name: "vadcopfinal", Label: "Vadcop Final Proof", Legend: "Wrap", Row: "Vadcop Final Proof", Phase: cluster.PhaseWrap},
	{Name: "compressed", Label: "Vadcop Final Compressed Proof", Legend: "Wrap", Row: "Vadcop Final Compressed Proof", Phase: cluster.PhaseWrap},
}

// proofTypeKinds is the kind of each proofman record kind, in record kind
// order. The first eight are the ProofType variants, and the two the timeline
// does not draw hold kindUnmapped.
var proofTypeKinds = []int{
	kindBasic, kindCompressor, kindRecursive1, kindRecursive2, kindVadcopFinal, kindCompressed,
	kindUnmapped, kindUnmapped,
	kindWitness, kindCommit, kindExecute, kindPrecalc, kindChallenge, kindRecompute, kindRecursionWitness, kindAggregationWitness,
}

// pipelineBreakdown pairs each breakdown label with the executor duration
// carrying it, in label order. Only the Execute bar carries them.
var pipelineBreakdown = []struct {
	label    string
	duration func(*api.ExecutorTime) uint64
}{
	{"Compute Minimal Trace", (*api.ExecutorTime).GetExecutionDuration},
	{"Plan Secondary", (*api.ExecutorTime).GetCountAndPlanDuration},
	{"Wait Plan Mem Cpp", (*api.ExecutorTime).GetCountAndPlanMoDuration},
}

// proofBreakdownLabels are the sections of one record, in the order the worker
// reports them. The first fourteen are the GPU sections of a proof or a commit.
// The rest are the host steps of the launch, the harvest, and the witness
// build, and the stream spans that no GPU section brackets. A witness build
// opens at its buffer take, so no section names the queue before it. A section
// a kind never measures stays zero and is left out.
var proofBreakdownLabels = []string{
	"Prepare Trace",
	"Commit Stage 1",
	"Calculate Accumulation Polynomials",
	"Calculate Intermediate Polynomials",
	"Commit Stage 2",
	"Quotient",
	"Evaluations",
	"FRI",
	"Load Custom Commits",
	"Trace Upload",
	"Load Const Tree",
	"Proof Readback",
	"Trace Unpack",
	"Compute LDE and Merkle",
	"Proof Write",
	"Witness Expansion",
	"Stream Wait",
	"Root Readback",
	"Circom Witness",
	"Prepare Witness Generation",
	"Witness Generation",
	"Reload Fixed Pols",
	"Harvest Wait",
	"Launch Prologue",
	"FFI Prologue",
	"Publics Staging",
	"GPU Enqueue Gap",
}

// breakdownLabels are the labels of pipelineBreakdown, then proofBreakdownLabels,
// in the same order.
var breakdownLabels = func() []string {
	labels := make([]string, len(pipelineBreakdown))
	for index, entry := range pipelineBreakdown {
		labels[index] = entry.label
	}
	return append(labels, proofBreakdownLabels...)
}()

// mapPipeline places every record the stats carry on the proof clock. The
// coordinator stamps each task result on receipt, and the origin age the worker
// reports sets the records of that result back from that stamp. A coordinator
// that reports no proof start, no task, or no record reports no timeline.
func mapPipeline(stats *api.ExecutionStats) (*cluster.Pipeline, error) {
	if stats.GetProofStart() == nil || len(stats.GetTasks()) == 0 {
		return nil, nil
	}

	builder := cluster.NewPipelineBuilder(mapRegistrations(stats.GetTasks()))
	proofStartMs := stats.GetProofStart().AsTime().UnixMilli()
	withAirgroup := foldsManyAirgroups(stats.GetTasks())
	for _, task := range stats.GetTasks() {
		switch task.GetPhase() {
		case api.JobPhase_JOB_PHASE_CONTRIBUTIONS, api.JobPhase_JOB_PHASE_PROVE, api.JobPhase_JOB_PHASE_RECURSE:
		default:
			return nil, fmt.Errorf("task of phase %s, which the timeline does not map", task.GetPhase())
		}
		endMs := task.GetCompletedAt().AsTime().UnixMilli() - proofStartMs
		if err := placeProofs(builder, task, endMs-int64(task.GetRecordsOriginAgeMs()), withAirgroup); err != nil {
			return nil, err
		}
	}
	pipeline, err := builder.Pipeline(pipelineKinds, breakdownLabels)
	if err != nil || len(pipeline.Tasks) == 0 {
		return nil, err
	}
	return pipeline, nil
}

// placeProofs places one bar per record the task reports, at originMs plus the
// offsets of each record. The worker takes the records before it replies, so no
// bar draws past the reply of its task. A fold in flight holds the origin
// across tasks, so originMs is signed and falls before the proof start whenever
// the origin is older than the task. The Execute bar also carries the executor
// durations, which the record does not measure as sections.
func placeProofs(builder *cluster.PipelineBuilder[string], task *api.TaskTiming, originMs int64, withAirgroup bool) error {
	for _, proof := range task.GetProofTimings() {
		kind, err := proofKind(proof.GetProofType())
		if err != nil {
			return err
		}
		startOffsetMs, endOffsetMs := int64(proof.GetStartOffsetMs()), int64(proof.GetEndOffsetMs())
		breakdown, err := mapProofBreakdown(proof.GetBreakdownMs())
		if err != nil {
			return err
		}
		if kind == kindExecute {
			breakdown = append(mapBreakdown(task.GetExecutorTime()), breakdown...)
		}
		id := proofID(kind, proof, withAirgroup)
		builder.Place(kind, task.GetWorkerId(), id, originMs+endOffsetMs, endOffsetMs-startOffsetMs, breakdown)
	}
	return nil
}

// foldsManyAirgroups reports whether the recursive2 proofs of the job name more
// than one airgroup. A fold index is unique inside one worker, so the airgroup
// tells a reader nothing until a second airgroup folds.
func foldsManyAirgroups(tasks []*api.TaskTiming) bool {
	recursive2Type := uint32(slices.Index(proofTypeKinds, kindRecursive2))
	airgroups := make(map[uint32]struct{}, 1)
	for _, task := range tasks {
		for _, proof := range task.GetProofTimings() {
			if proof.GetProofType() == recursive2Type {
				airgroups[proof.GetAirgroupId()] = struct{}{}
			}
		}
	}
	return len(airgroups) > 1
}

// proofKind is the kind of one proofman record kind. A kind the timeline does
// not draw fails the record instead of drawing on the row of another kind.
func proofKind(proofType uint32) (int, error) {
	if int(proofType) >= len(proofTypeKinds) || proofTypeKinds[proofType] == kindUnmapped {
		return 0, fmt.Errorf("proof of type %d, which the timeline does not map", proofType)
	}
	return proofTypeKinds[proofType], nil
}

// proofID names one record by the instance it belongs to, by the ongoing index
// for a fold, and by nothing for a root step of the contributions phase and for
// the two final proofs. The records of one instance share an id, which the
// frontend folds onto one row per row group. A fold id carries the airgroup
// only when the job folds more than one.
func proofID(kind int, proof *api.ProofTiming, withAirgroup bool) string {
	switch kind {
	case kindWitness, kindCommit, kindRecompute, kindRecursionWitness, kindBasic, kindCompressor, kindRecursive1:
		return fmt.Sprintf("%s #%d", proof.GetAirName(), proof.GetId())
	case kindAggregationWitness, kindRecursive2:
		if withAirgroup {
			return fmt.Sprintf("airgroup %d #%d", proof.GetAirgroupId(), proof.GetId())
		}
		return fmt.Sprintf("#%d", proof.GetId())
	default:
		return ""
	}
}

// mapRegistrations names every worker the task list holds, ordered by rank and
// then by id. One worker runs per host, so every worker is its own node and
// the nodes read node1 and up in rank order.
func mapRegistrations(tasks []*api.TaskTiming) []cluster.Registration[string] {
	named := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		named[task.GetWorkerId()] = struct{}{}
	}
	workerIDs := slices.SortedFunc(maps.Keys(named), func(a, b string) int {
		return cmp.Or(cmp.Compare(workerRank(a), workerRank(b)), cmp.Compare(a, b))
	})
	registrations := make([]cluster.Registration[string], len(workerIDs))
	for index, workerID := range workerIDs {
		registrations[index] = cluster.Registration[string]{Key: workerID, Name: workerID, Host: workerID}
	}
	return registrations
}

// workerRank is the index a worker id of the shape cluster.WorkerNameFormat
// carries, and the largest int for an id of another shape, which keeps such an
// id last and ordered by name.
func workerRank(workerID string) int {
	var rank int
	var label string
	if _, err := fmt.Sscanf(workerID, cluster.WorkerNameFormat, &rank, &label); err != nil {
		return math.MaxInt
	}
	return rank
}

// mapBreakdown indexes the executor durations the frontend labels, in label
// order. A duration the task reports as zero is left out.
func mapBreakdown(executorTime *api.ExecutorTime) [][2]int64 {
	var breakdown [][2]int64
	for index, entry := range pipelineBreakdown {
		if duration := entry.duration(executorTime); duration > 0 {
			breakdown = append(breakdown, [2]int64{int64(index), int64(duration)})
		}
	}
	return breakdown
}

// mapProofBreakdown indexes the sections of one record, which follow the
// executor labels. A section the worker reports as zero is left out. A record
// of more sections than the labels name fails, because no label names the
// sections past the list.
func mapProofBreakdown(sections []uint32) ([][2]int64, error) {
	if len(sections) > len(proofBreakdownLabels) {
		return nil, fmt.Errorf("record of %d sections, which the timeline does not map", len(sections))
	}
	var breakdown [][2]int64
	for index, duration := range sections {
		if duration > 0 {
			breakdown = append(breakdown, [2]int64{int64(len(pipelineBreakdown) + index), int64(duration)})
		}
	}
	return breakdown, nil
}
