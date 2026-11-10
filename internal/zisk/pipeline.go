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
	kindCompressorWitness
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
	{Name: "compressorwitness", Label: "Compressor Witness Generation", Legend: "Witness Generation", Row: "Proof", Phase: cluster.PhaseWitness},
}

// proofTypeKinds is the kind of each proofman record kind, in record kind
// order. The first eight are the ProofType variants, and the two the timeline
// does not draw hold kindUnmapped.
var proofTypeKinds = []int{
	kindBasic, kindCompressor, kindRecursive1, kindRecursive2, kindVadcopFinal, kindCompressed,
	kindUnmapped, kindUnmapped,
	kindWitness, kindCommit, kindExecute, kindPrecalc, kindChallenge, kindRecompute, kindRecursionWitness, kindAggregationWitness,
	kindCompressorWitness,
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
	"CIRCOM_WITNESS", "COMPUTING_WC", "GENERATING_WC", "GEN_PROOF", "GEN_RECURSIVE_PROOF", "GET_CONTRIBUTION_AIR", "POOL_WAIT_WITNESS", "PREPARING_WC", "STAGING_WC",
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
	rounds := foldRounds(stats.GetTasks())
	labels, labelIndex := spanLabels(stats.GetTasks())
	for _, task := range stats.GetTasks() {
		switch task.GetPhase() {
		case api.JobPhase_JOB_PHASE_CONTRIBUTIONS, api.JobPhase_JOB_PHASE_PROVE, api.JobPhase_JOB_PHASE_RECURSE:
		default:
			return nil, fmt.Errorf("task of phase %s, which the timeline does not map", task.GetPhase())
		}
		if err := placeProofs(builder, task, proofStartMs, withAirgroup, rounds[task], labelIndex); err != nil {
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
// coordinator clock. Any other task draws each bar over the worker stamps of
// its record. With span labels, a bar of a record with spans carries its spans,
// the network transport of a round trip at the two ends of the bar, and the
// time of the bar they leave uncovered, so its segments close on the bar. The
// dispatch leg takes what the two clocks tell, up to the whole transport, and
// the spans move by the same skew, so a skew never cuts a span. A record with
// no span carries no breakdown. Without span labels, a bar carries the sections
// of its record, the Execute bar also the executor durations, and a round trip
// also the network transport.
func placeProofs(builder *cluster.PipelineBuilder[string], task *api.TaskTiming, proofStartMs int64, withAirgroup bool, round int, labelIndex map[string]int64) error {
	roundTrip := len(task.GetProofTimings()) == 1 && task.GetCoordinatorStart() != 0
	for _, proof := range task.GetProofTimings() {
		kind, err := proofKind(proof.GetProofType())
		if err != nil {
			return err
		}
		startMs, endMs := int64(proof.GetStart()), int64(proof.GetEnd())
		transportMs, dispatchMs, skewMs := int64(0), int64(0), int64(0)
		if roundTrip {
			startMs, endMs = int64(task.GetCoordinatorStart()), int64(task.GetCoordinatorEnd())
			workerStartMs := int64(task.GetWorkerStart())
			transportMs = cluster.NetworkTransportMs(startMs, workerStartMs, int64(task.GetWorkerEnd()), endMs)
			dispatchMs = min(max(workerStartMs-startMs, 0), transportMs)
			skewMs = workerStartMs - startMs - dispatchMs
		}
		var breakdown [][2]int64
		var segments [][4]int64
		switch {
		case labelIndex == nil:
			breakdown = mapProofBreakdown(proof.GetBreakdownMs())
			if kind == kindExecute {
				breakdown = append(mapBreakdown(task.GetExecutorTime()), breakdown...)
			}
			if transportMs > 0 {
				breakdown = append(breakdown, [2]int64{int64(len(breakdownLabels) - 1), transportMs})
			}
		case len(proof.GetSpans()) > 0:
			fromUs, toUs := max(startMs-proofStartMs, 0)*1000, (endMs-proofStartMs)*1000
			workFromUs, workToUs := fromUs+dispatchMs*1000, toUs-(transportMs-dispatchMs)*1000
			if segments, err = mapSpans(proof.GetSpans(), task.GetNames(), labelIndex, (proofStartMs+skewMs)*1000, workFromUs, workToUs); err != nil {
				return err
			}
			segments = append(segments, unattributedSegments(segments, labelIndex[unattributedLabel], workFromUs, workToUs)...)
			transport := labelIndex[cluster.NetworkTransportLabel]
			for _, leg := range [][2]int64{{fromUs, workFromUs}, {workToUs, toUs}} {
				if leg[0] < leg[1] {
					segments = append(segments, [4]int64{transport, leg[0], leg[1] - leg[0], 0})
				}
			}
		}
		id := proofID(kind, proof, withAirgroup, round)
		builder.PlaceSegments(kind, task.GetWorkerId(), id, endMs-proofStartMs, endMs-startMs, breakdown, segments)
	}
	return nil
}

// spanLabels are the Unattributed label, the network transport label, and the
// label of every span name the tasks carry, in first sight order, indexed by
// label. Stats that name no span keep breakdownLabels and a nil index.
func spanLabels(tasks []*api.TaskTiming) ([]string, map[string]int64) {
	if !slices.ContainsFunc(tasks, func(task *api.TaskTiming) bool { return len(task.GetNames()) > 0 }) {
		return breakdownLabels, nil
	}
	labels := []string{unattributedLabel, cluster.NetworkTransportLabel}
	labelIndex := map[string]int64{unattributedLabel: 0, cluster.NetworkTransportLabel: 1}
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

// foldRounds numbers the tasks of a worker that carry folds, from one, for a
// worker that folds in more than one task. Fold ids restart on every
// aggregation round of a worker, so the round keeps two folds of one id apart,
// and a worker that folds once needs no round.
func foldRounds(tasks []*api.TaskTiming) map[*api.TaskTiming]int {
	foldTypes := []uint32{uint32(slices.Index(proofTypeKinds, kindAggregationWitness)), uint32(slices.Index(proofTypeKinds, kindRecursive2))}
	byWorker := make(map[string][]*api.TaskTiming)
	for _, task := range tasks {
		if slices.ContainsFunc(task.GetProofTimings(), func(proof *api.ProofTiming) bool { return slices.Contains(foldTypes, proof.GetProofType()) }) {
			byWorker[task.GetWorkerId()] = append(byWorker[task.GetWorkerId()], task)
		}
	}
	rounds := make(map[*api.TaskTiming]int)
	for _, folding := range byWorker {
		if len(folding) > 1 {
			for index, task := range folding {
				rounds[task] = index + 1
			}
		}
	}
	return rounds
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
// only when the job folds more than one, and the round only when its worker
// folds in more than one task.
func proofID(kind int, proof *api.ProofTiming, withAirgroup bool, round int) string {
	switch kind {
	case kindWitness, kindCommit, kindRecompute, kindRecursionWitness, kindCompressorWitness, kindBasic, kindCompressor, kindRecursive1:
		return fmt.Sprintf("%s #%d", proof.GetAirName(), proof.GetId())
	case kindAggregationWitness, kindRecursive2:
		id := fmt.Sprintf("#%d", proof.GetId())
		if withAirgroup {
			id = fmt.Sprintf("airgroup %d %s", proof.GetAirgroupId(), id)
		}
		if round > 0 {
			id = fmt.Sprintf("round %d %s", round, id)
		}
		return id
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
