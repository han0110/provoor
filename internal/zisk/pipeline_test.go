package zisk

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/zisk/api"
)

// sections builds the breakdown of one record, keyed by the name of each
// section index.
func sections(measured map[int]uint32) map[string]uint32 {
	breakdown := make(map[string]uint32, len(measured))
	for index, duration := range measured {
		breakdown[proofBreakdownLabels[index]] = duration
	}
	return breakdown
}

// at is the Unix millisecond stamp offsetMs after proofStart.
func at(proofStart time.Time, offsetMs int64) uint64 {
	return uint64(proofStart.UnixMilli() + offsetMs)
}

// The stats carry the contributions task of two workers, each reporting the
// EXECUTE record of its own emulation, and the tasks arrive out of rank order.
// A task of one record draws its bar from the coordinator dispatch to the
// coordinator receipt, and the two legs outside the worker stamps make the
// network transport.
func TestPipelineMapsTheCoordinatorStats(t *testing.T) {
	proofStart := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	executorTime := func(execution, countAndPlan, memoryCountAndPlan uint64) *api.ExecutorTime {
		return &api.ExecutorTime{
			ExecutionDuration:      execution,
			CountAndPlanDuration:   countAndPlan,
			CountAndPlanMoDuration: memoryCountAndPlan,
		}
	}
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(proofStart),
		Tasks: []*api.TaskTiming{
			{
				WorkerId: "worker_1-gpu_0", Phase: api.JobPhase_JOB_PHASE_CONTRIBUTIONS,
				CoordinatorStart: at(proofStart, 380), WorkerStart: at(proofStart, 400),
				WorkerEnd: at(proofStart, 2150), CoordinatorEnd: at(proofStart, 2160), ComputeDurationMs: 1750,
				ExecutorTime: executorTime(1380, 210, 128),
				ProofTimings: []*api.ProofTiming{{ProofType: 10, Start: at(proofStart, 400), End: at(proofStart, 2150)}},
			},
			{
				WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_CONTRIBUTIONS,
				CoordinatorStart: at(proofStart, 50), WorkerStart: at(proofStart, 60),
				WorkerEnd: at(proofStart, 1870), CoordinatorEnd: at(proofStart, 1884), ComputeDurationMs: 1801,
				ExecutorTime: executorTime(1421, 214, 132),
				ProofTimings: []*api.ProofTiming{{ProofType: 10, Start: at(proofStart, 65), End: at(proofStart, 1866)}},
			},
		},
	}

	pipeline, err := mapPipeline(stats)
	if err != nil {
		t.Fatal(err)
	}

	wantWorkers := []cluster.Worker{{Name: "worker_0-gpu_0", Node: "node1"}, {Name: "worker_1-gpu_0", Node: "node2"}}
	if !slices.Equal(pipeline.Workers, wantWorkers) {
		t.Errorf("workers = %+v, want %+v", pipeline.Workers, wantWorkers)
	}
	if !slices.Equal(pipeline.Kinds, pipelineKinds) || !slices.Equal(pipeline.Breakdown, breakdownLabels) {
		t.Errorf("pipeline = %+v", pipeline)
	}
	wantTasks := `[` +
		`[0,0,"",50,1834,[[0,1421],[1,214],[2,132],[30,24]]],` +
		`[0,1,"",380,1780,[[0,1380],[1,210],[2,128],[30,30]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

// One worker reports every kind of one job, and every bar draws over the worker
// stamps of its record because each task carries more than one record. The
// contributions task carries the
// three root steps and the witness and the contribution of instance 137, the
// prove task carries the recompute, the three proofs and the two circom
// witnesses of that instance and one fold, and the final recurse task carries
// the two final proofs. Every record of the instance shares one id, and the
// row group of its kind splits the contributions phase from the proofs.
func TestPipelinePlacesEveryRecord(t *testing.T) {
	proofStart := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	record := func(offsetMs int64) uint64 { return at(proofStart, 1000+offsetMs) }
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(proofStart),
		Tasks: []*api.TaskTiming{
			{
				WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_CONTRIBUTIONS,
				CoordinatorEnd: at(proofStart, 30000), ComputeDurationMs: 29800,
				ExecutorTime: &api.ExecutorTime{ExecutionDuration: 143, CountAndPlanDuration: 12, CountAndPlanMoDuration: 23},
				ProofTimings: []*api.ProofTiming{
					{ProofType: 10, Start: record(0), End: record(190), BreakdownMs: sections(map[int]uint32{21: 8})},
					{
						ProofType: 8, Id: 137, AirName: "Main", Start: record(120), End: record(900),
						BreakdownMs: sections(map[int]uint32{20: 700}),
					},
					{ProofType: 11, Start: record(200), End: record(260)},
					{
						ProofType: 9, Id: 137, AirName: "Main", Start: record(905), End: record(1100),
						BreakdownMs: sections(map[int]uint32{2: 20, 9: 60, 12: 30, 13: 70, 16: 5, 17: 2}),
					},
					{ProofType: 12, Start: record(1150), End: record(1180)},
				},
			},
			{
				WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_PROVE,
				CoordinatorEnd: at(proofStart, 140000), ComputeDurationMs: 110000,
				ProofTimings: []*api.ProofTiming{
					{
						ProofType: 13, Id: 137, AirName: "Main", Start: record(2000), End: record(5200),
						BreakdownMs: sections(map[int]uint32{19: 400, 20: 2300}),
					},
					{
						ProofType: 0, Id: 137, AirName: "Main", Start: record(5300), End: record(5700),
						BreakdownMs: sections(map[int]uint32{
							0: 30, 1: 12, 2: 4, 3: 1, 4: 26, 5: 55, 6: 31, 7: 55, 8: 9, 9: 14, 10: 21, 11: 3, 14: 6,
							22: 5, 23: 3, 24: 1, 25: 2, 26: 4,
						}),
					},
					{ProofType: 14, Id: 137, AirName: "Main", Start: record(5700), End: record(7100)},
					{
						ProofType: 1, Id: 137, AirName: "Main", Start: record(7100), End: record(7500),
						BreakdownMs: sections(map[int]uint32{0: 20, 3: 6, 15: 900}),
					},
					{ProofType: 14, Id: 137, AirName: "Main", Start: record(7500), End: record(7680)},
					{
						ProofType: 2, Id: 137, AirName: "Main", Start: record(7680), End: record(8000),
						BreakdownMs: sections(map[int]uint32{0: 10, 2: 2, 4: 5, 7: 8, 10: 8, 11: 2, 14: 2, 15: 120, 16: 4}),
					},
					{ProofType: 15, Id: 4, Start: record(8000), End: record(8180)},
					{ProofType: 3, Id: 4, Start: record(8200), End: record(8500)},
				},
			},
			{
				WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_RECURSE,
				CoordinatorEnd: at(proofStart, 141000), ComputeDurationMs: 200, Step: 1,
				ProofTimings: []*api.ProofTiming{
					{
						ProofType: 4, Start: record(9000), End: record(9114),
						BreakdownMs: sections(map[int]uint32{0: 10, 14: 2, 15: 20, 16: 3, 18: 40}),
					},
					{
						ProofType: 5, Start: record(9120), End: record(9179),
						BreakdownMs: sections(map[int]uint32{15: 10, 16: 1, 18: 20}),
					},
				},
			},
		},
	}

	pipeline, err := mapPipeline(stats)
	if err != nil {
		t.Fatal(err)
	}

	wantTasks := `[` +
		`[0,0,"",1000,190,[[0,143],[1,12],[2,23],[24,8]]],` +
		`[2,0,"Main #137",1120,780,[[23,700]]],` +
		`[1,0,"",1200,60,[]],` +
		`[7,0,"Main #137",1905,195,[[5,20],[12,60],[15,30],[16,70],[19,5],[20,2]]],` +
		`[6,0,"",2150,30,[]],` +
		`[3,0,"Main #137",3000,3200,[[22,400],[23,2300]]],` +
		`[8,0,"Main #137",6300,400,[[3,30],[4,12],[5,4],[6,1],[7,26],[8,55],[9,31],[10,55],[11,9],[12,14],[13,21],[14,3],[17,6],` +
		`[25,5],[26,3],[27,1],[28,2],[29,4]]],` +
		`[4,0,"Main #137",6700,1400,[]],` +
		`[9,0,"Main #137",8100,400,[[3,20],[6,6],[18,900]]],` +
		`[4,0,"Main #137",8500,180,[]],` +
		`[10,0,"Main #137",8680,320,[[3,10],[5,2],[7,5],[10,8],[13,8],[14,2],[17,2],[18,120],[19,4]]],` +
		`[5,0,"#4",9000,180,[]],` +
		`[11,0,"#4",9200,300,[]],` +
		`[12,0,"",10000,114,[[3,10],[17,2],[18,20],[19,3],[21,40]]],` +
		`[13,0,"",10120,59,[[18,10],[19,1],[21,20]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}

	rows := make(map[string][]string)
	for _, task := range pipeline.Tasks {
		row := pipeline.Kinds[task.Kind].Row + " " + task.ID
		rows[row] = append(rows[row], pipeline.Kinds[task.Kind].Label)
	}
	wantRows := map[string][]string{
		"Execute ":                          {"Execute"},
		"Pre Calculate Witness Generation ": {"Pre Calculate Witness Generation"},
		"Calculate Internal Contributions ": {"Calculate Internal Contributions"},
		"Contribution Main #137":            {"Witness Generation", "Contribution"},
		"Proof Main #137": {
			"Witness Recompute", "Basic Proof", "Recursion Witness Generation", "Compressor Proof",
			"Recursion Witness Generation", "Recursive1 Proof",
		},
		"Fold #4":                        {"Aggregation Witness Generation", "Recursive2 Proof"},
		"Vadcop Final Proof ":            {"Vadcop Final Proof"},
		"Vadcop Final Compressed Proof ": {"Vadcop Final Compressed Proof"},
	}
	if !maps.EqualFunc(rows, wantRows, slices.Equal) {
		t.Errorf("rows = %v, want %v", rows, wantRows)
	}
}

// The kinds of one legend entry share its shade, so they share a phase.
func TestPipelineKindsOfOneLegendEntryShareAPhase(t *testing.T) {
	phases := make(map[string]string)
	for _, kind := range pipelineKinds {
		if phase, seen := phases[kind.Legend]; seen && phase != kind.Phase {
			t.Errorf("legend %q draws in phases %s and %s", kind.Legend, phase, kind.Phase)
		}
		phases[kind.Legend] = kind.Phase
	}
}

// The worker reports one section per PROOF_TIMING_SECTIONS of the proofman
// fork, and every section carries a label.
func TestPipelineLabelsEverySection(t *testing.T) {
	if len(proofBreakdownLabels) != 27 {
		t.Errorf("labels = %d, want 27", len(proofBreakdownLabels))
	}
}

// A job that folds two airgroups names the airgroup on every fold, because the
// index alone reads the same on both. The aggregation witness of a fold carries
// the id of the fold it feeds.
func TestPipelineNamesTheAirgroupOfEveryFoldOfManyAirgroups(t *testing.T) {
	proofStart := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(proofStart),
		Tasks: []*api.TaskTiming{{
			WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_RECURSE,
			CoordinatorEnd: at(proofStart, 1000), ComputeDurationMs: 200, Step: 1,
			ProofTimings: []*api.ProofTiming{
				{Id: 4, ProofType: 15, AirgroupId: 0, Start: at(proofStart, 50), End: at(proofStart, 100)},
				{Id: 4, ProofType: 3, AirgroupId: 0, Start: at(proofStart, 100), End: at(proofStart, 300)},
				{Id: 4, ProofType: 3, AirgroupId: 1, Start: at(proofStart, 300), End: at(proofStart, 500)},
			},
		}},
	}

	pipeline, err := mapPipeline(stats)
	if err != nil {
		t.Fatal(err)
	}

	wantTasks := `[` +
		`[5,0,"airgroup 0 #4",50,50,[]],` +
		`[11,0,"airgroup 0 #4",100,200,[]],` +
		`[11,0,"airgroup 1 #4",300,200,[]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

// A record kind of the two ProofType variants the timeline leaves out, and one
// past the table, each fail the proof.
func TestPipelineOfAnUnmappedProofTypeFails(t *testing.T) {
	for _, proofType := range []uint32{6, 7, uint32(len(proofTypeKinds))} {
		stats := &api.ExecutionStats{
			ProofStart: timestamppb.New(time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)),
			Tasks: []*api.TaskTiming{{
				WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_PROVE,
				CoordinatorEnd: at(time.Date(2026, 9, 4, 10, 0, 1, 0, time.UTC), 0), ComputeDurationMs: 100,
				ProofTimings: []*api.ProofTiming{{ProofType: proofType}},
			}},
		}

		pipeline, err := mapPipeline(stats)
		if pipeline != nil || err == nil {
			t.Errorf("pipeline of type %d = %+v, %v, want an error", proofType, pipeline, err)
		}
	}
}

// A section of a name no label carries is dropped, and the record keeps the
// sections the labels name.
func TestPipelineDropsASectionOfAnUnknownName(t *testing.T) {
	proofStart := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(proofStart),
		Tasks: []*api.TaskTiming{{
			WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_PROVE,
			CoordinatorStart: at(proofStart, 100), WorkerStart: at(proofStart, 100),
			WorkerEnd: at(proofStart, 500), CoordinatorEnd: at(proofStart, 500), ComputeDurationMs: 400,
			ProofTimings: []*api.ProofTiming{{
				ProofType: 0, Id: 137, AirName: "Main", Start: at(proofStart, 100), End: at(proofStart, 500),
				BreakdownMs: map[string]uint32{"FRI": 40, "Unknown Section": 60},
			}},
		}},
	}

	pipeline, err := mapPipeline(stats)
	if err != nil {
		t.Fatal(err)
	}

	wantTasks := `[[8,0,"Main #137",100,400,[[10,40]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

// A task of one record and no dispatch stamp draws its bar over the worker
// stamps of its record and adds no network transport.
func TestPipelinePlacesATaskWithoutADispatchStampOverItsRecord(t *testing.T) {
	proofStart := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(proofStart),
		Tasks: []*api.TaskTiming{{
			WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_PROVE,
			WorkerStart: at(proofStart, 100), WorkerEnd: at(proofStart, 500),
			CoordinatorEnd: at(proofStart, 600), ComputeDurationMs: 400,
			ProofTimings: []*api.ProofTiming{{
				ProofType: 0, Id: 137, AirName: "Main", Start: at(proofStart, 100), End: at(proofStart, 500),
				BreakdownMs: map[string]uint32{"FRI": 40},
			}},
		}},
	}

	pipeline, err := mapPipeline(stats)
	if err != nil {
		t.Fatal(err)
	}

	wantTasks := `[[8,0,"Main #137",100,400,[[10,40]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

func TestPipelineOfAnUnmappedPhaseFails(t *testing.T) {
	stats := &api.ExecutionStats{
		ProofStart: timestamppb.New(time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)),
		Tasks: []*api.TaskTiming{{
			WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_UNSPECIFIED,
			CoordinatorEnd: at(time.Date(2026, 9, 4, 10, 0, 1, 0, time.UTC), 0), ComputeDurationMs: 100,
		}},
	}

	pipeline, err := mapPipeline(stats)
	if pipeline != nil || err == nil {
		t.Errorf("pipeline = %+v, %v, want an error", pipeline, err)
	}
}

func TestPipelineOfACoordinatorWithoutTimingsIsEmpty(t *testing.T) {
	proofStart := timestamppb.New(time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC))
	withoutRecords := &api.ExecutionStats{
		ProofStart: proofStart,
		Tasks: []*api.TaskTiming{{
			WorkerId: "worker_0-gpu_0", Phase: api.JobPhase_JOB_PHASE_PROVE,
			CoordinatorEnd: at(time.Date(2026, 9, 4, 10, 0, 1, 0, time.UTC), 0), ComputeDurationMs: 100,
		}},
	}
	for _, stats := range []*api.ExecutionStats{{}, {ProofStart: proofStart}, withoutRecords} {
		pipeline, err := mapPipeline(stats)
		if pipeline != nil || err != nil {
			t.Errorf("pipeline = %+v, %v, want no timeline", pipeline, err)
		}
	}
}
