package zisk

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

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
	kindThread
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
	{Name: "thread", Label: "Thread", Legend: "Thread", Row: "Thread", Phase: cluster.PhaseThread},
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

// proofBreakdownLabels are the section names of one record, in the order of
// PROOF_TIMING_SECTION_NAMES in proofman. The first fourteen are the GPU
// sections of a proof or a commit.
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
// in the same order, then the network transport label.
var breakdownLabels = func() []string {
	labels := make([]string, len(pipelineBreakdown))
	for index, entry := range pipelineBreakdown {
		labels[index] = entry.label
	}
	return append(append(labels, proofBreakdownLabels...), cluster.NetworkTransportLabel)
}()

// unattributedLabel labels the time of a record that no span covers.
const unattributedLabel = "Unattributed"

// formattedTimerNames are the upstream timers that format ids after their
// name and an underscore. Their spans share the timer name as label, and any
// other span keeps its own name.
var formattedTimerNames = []string{
	"CIRCOM_WITNESS", "COMPUTING_WC", "GENERATING_WC", "GEN_PROOF", "GET_CONTRIBUTION_AIR", "POOL_WAIT_WITNESS", "PREPARING_WC",
}

// mapPipeline places every record the stats carry on the proof clock, which
// starts at the proof start. A coordinator that reports no proof start, no
// task, or no record reports no timeline. Stats that name spans come from
// ZisK 1.4 and later, and their records draw their spans as segments.
func mapPipeline(stats *api.ExecutionStats) (*cluster.Pipeline, error) {
	if stats.GetProofStart() == nil || len(stats.GetTasks()) == 0 {
		return nil, nil
	}

	builder := cluster.NewPipelineBuilder(mapRegistrations(stats.GetTasks()))
	proofStartMs := stats.GetProofStart().AsTime().UnixMilli()
	withAirgroup := foldsManyAirgroups(stats.GetTasks())
	labels, labelIndex := spanLabels(stats.GetTasks())
	for _, task := range stats.GetTasks() {
		switch task.GetPhase() {
		case api.JobPhase_JOB_PHASE_CONTRIBUTIONS, api.JobPhase_JOB_PHASE_PROVE, api.JobPhase_JOB_PHASE_RECURSE:
		default:
			return nil, fmt.Errorf("task of phase %s, which the timeline does not map", task.GetPhase())
		}
		if err := placeProofs(builder, task, proofStartMs, withAirgroup, labelIndex); err != nil {
			return nil, err
		}
		if err := placeThreads(builder, task, proofStartMs, labelIndex); err != nil {
			return nil, err
		}
	}
	pipeline, err := builder.Pipeline(pipelineKinds, labels)
	if err != nil || len(pipeline.Tasks) == 0 {
		return nil, err
	}
	return pipeline, nil
}

// placeProofs places one bar per record the task reports. A task of one record
// and a dispatch stamp draws its bar over the whole round trip on the
// coordinator clock and adds the network transport to its sections. Any other
// task draws each bar over the worker stamps of its record. The Execute bar
// also carries the executor durations, which the record does not measure as
// sections. With span labels, every bar also carries the spans of its record
// and the time of the record they leave uncovered.
func placeProofs(builder *cluster.PipelineBuilder[string], task *api.TaskTiming, proofStartMs int64, withAirgroup bool, labelIndex map[string]int64) error {
	roundTrip := len(task.GetProofTimings()) == 1 && task.GetCoordinatorStart() != 0
	for _, proof := range task.GetProofTimings() {
		kind, err := proofKind(proof.GetProofType())
		if err != nil {
			return err
		}
		startMs, endMs := int64(proof.GetStart()), int64(proof.GetEnd())
		breakdown := mapProofBreakdown(proof.GetBreakdownMs())
		if kind == kindExecute {
			breakdown = append(mapBreakdown(task.GetExecutorTime()), breakdown...)
		}
		if roundTrip {
			startMs, endMs = int64(task.GetCoordinatorStart()), int64(task.GetCoordinatorEnd())
			transportMs := cluster.NetworkTransportMs(startMs, int64(task.GetWorkerStart()), int64(task.GetWorkerEnd()), endMs)
			if transportMs > 0 {
				breakdown = append(breakdown, [2]int64{int64(len(breakdownLabels) - 1), transportMs})
			}
		}
		var segments [][4]int64
		if labelIndex != nil {
			fromUs, toUs := max(int64(proof.GetStart())-proofStartMs, 0)*1000, (int64(proof.GetEnd())-proofStartMs)*1000
			if segments, err = mapSpans(proof.GetSpans(), task.GetNames(), labelIndex, proofStartMs*1000, fromUs, toUs); err != nil {
				return err
			}
			segments = append(segments, unattributedSegments(segments, labelIndex[unattributedLabel], fromUs, toUs)...)
		}
		id := proofID(kind, proof, withAirgroup)
		builder.PlaceSegments(kind, task.GetWorkerId(), id, endMs-proofStartMs, endMs-startMs, breakdown, segments)
	}
	return nil
}

// placeThreads places one bar per thread of the task that runs no record,
// over the extent of its spans. The bars of one thread name share a row.
func placeThreads(builder *cluster.PipelineBuilder[string], task *api.TaskTiming, proofStartMs int64, labelIndex map[string]int64) error {
	for _, thread := range task.GetThreads() {
		segments, err := mapSpans(thread.GetSpans(), task.GetNames(), labelIndex, proofStartMs*1000, 0, math.MaxInt64)
		if err != nil {
			return err
		}
		if len(segments) == 0 {
			continue
		}
		fromUs, toUs := int64(math.MaxInt64), int64(0)
		for _, segment := range segments {
			fromUs, toUs = min(fromUs, segment[1]), max(toUs, segment[1]+segment[2])
		}
		endMs := (toUs + 999) / 1000
		builder.PlaceSegments(kindThread, task.GetWorkerId(), thread.GetThread(), endMs, endMs-fromUs/1000, nil, segments)
	}
	return nil
}

// spanLabels extends breakdownLabels with the Unattributed label and the label
// of every span name the tasks carry, in first sight order, and indexes them by
// label. Stats that name no span keep breakdownLabels and a nil index.
func spanLabels(tasks []*api.TaskTiming) ([]string, map[string]int64) {
	if !slices.ContainsFunc(tasks, func(task *api.TaskTiming) bool { return len(task.GetNames()) > 0 }) {
		return breakdownLabels, nil
	}
	labels := append(slices.Clone(breakdownLabels), unattributedLabel)
	labelIndex := map[string]int64{unattributedLabel: int64(len(labels) - 1)}
	for _, task := range tasks {
		for _, name := range task.GetNames() {
			label := spanLabel(name)
			if _, seen := labelIndex[label]; !seen {
				labelIndex[label] = int64(len(labels))
				labels = append(labels, label)
			}
		}
	}
	return labels, labelIndex
}

// spanLabel is the timer name of a formatted span name, and the name itself
// otherwise.
func spanLabel(name string) string {
	for _, timer := range formattedTimerNames {
		if strings.HasPrefix(name, timer+"_") {
			return timer
		}
	}
	return name
}

// mapSpans places spans as segments in microseconds from the proof start,
// each clipped to the window from fromUs to toUs. A span the window leaves
// out is dropped, and a span of a name the table does not hold fails.
func mapSpans(spans []*api.Span, names []string, labelIndex map[string]int64, proofStartUs, fromUs, toUs int64) ([][4]int64, error) {
	segments := [][4]int64{}
	for _, span := range spans {
		if int(span.GetName()) >= len(names) {
			return nil, fmt.Errorf("span of name %d, which a table of %d names does not hold", span.GetName(), len(names))
		}
		startUs := max(int64(span.GetStartUs())-proofStartUs, fromUs)
		endUs := min(int64(span.GetEndUs())-proofStartUs, toUs)
		if startUs < endUs {
			segments = append(segments, [4]int64{labelIndex[spanLabel(names[span.GetName()])], startUs, endUs - startUs, int64(span.GetDepth())})
		}
	}
	return segments, nil
}

// unattributedSegments are the gaps the segments leave in the window from
// fromUs to toUs, one segment of the label at depth zero per gap.
func unattributedSegments(segments [][4]int64, label, fromUs, toUs int64) [][4]int64 {
	byStart := slices.SortedFunc(slices.Values(segments), func(a, b [4]int64) int { return cmp.Compare(a[1], b[1]) })
	var gaps [][4]int64
	coveredUs := fromUs
	for _, segment := range append(byStart, [4]int64{label, toUs, 0, 0}) {
		if segment[1] > coveredUs {
			gaps = append(gaps, [4]int64{label, coveredUs, segment[1] - coveredUs, 0})
		}
		coveredUs = max(coveredUs, segment[1]+segment[2])
	}
	return gaps
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

// mapProofBreakdown indexes the sections of one record by name, which follow
// the executor labels, in label order. A section the worker reports as zero is
// left out, and a section of a name no label carries is dropped.
func mapProofBreakdown(sections map[string]uint32) [][2]int64 {
	var breakdown [][2]int64
	for index, label := range proofBreakdownLabels {
		if duration := sections[label]; duration > 0 {
			breakdown = append(breakdown, [2]int64{int64(len(pipelineBreakdown) + index), int64(duration)})
		}
	}
	return breakdown
}
