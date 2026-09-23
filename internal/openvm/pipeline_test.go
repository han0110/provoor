package openvm

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/han0110/provoor/internal/cluster"
)

// placedTasks maps the view onto the timeline of worker 0 and encodes the tasks.
func placedTasks(t *testing.T, view pipelineView) string {
	t.Helper()
	pipeline, err := mapPipeline(&view, []workerRegistration{{WorkerURL: "http://10.0.0.1:8001"}})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	return string(tasks)
}

// The view carries a proof whose second app proof fast forwards from before
// the proof start and meters its segment earlier still, so the metered task
// falls away. A leaf reports a sub metric the frontend does not label, and the
// final internal proof compresses and reports the wrap spans apart from its
// own. The third app proof does not fast forward at all. The workers register
// out of order on two hosts.
func TestPipelineMapsTheCoordinatorView(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	completedAt := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		AppProofs: []taskTiming{
			{
				DispatchedAtMs: completedAt(10), WorkerStartMs: completedAt(20), WorkerEndMs: completedAt(1690),
				CompletedAtMs: completedAt(1700), SegmentStart: 24, SegmentEnd: 24,
				QueueWaitMs: 200, MeteredTimeMs: 300,
				ProveTimeMs: 1140, FastForwardTimeMs: 40, StarkProveTimeMs: 1100,
				SubMetrics: map[string]float64{
					"execute_preflight_time_ms":                 30.2,
					"postflight_time_ms":                        60,
					"trace_gen_time_ms":                         40,
					"prover.main_trace_commit_time_ms":          90,
					"prover.rap_constraints.logup_gkr_time_ms":  270,
					"prover.rap_constraints.round0_time_ms":     280,
					"prover.rap_constraints.mle_rounds_time_ms": 50,
					"prover.openings_time_ms":                   250,
				},
			},
			{
				WorkerID: 5, WorkerStartMs: completedAt(-350), WorkerEndMs: completedAt(295),
				CompletedAtMs: completedAt(300), SegmentStart: 7, SegmentEnd: 7,
				MeteredTimeMs: 100, ProveTimeMs: 690, FastForwardTimeMs: 400, StarkProveTimeMs: 250,
			},
			{
				WorkerID: 5, WorkerStartMs: completedAt(700), WorkerEndMs: completedAt(900),
				CompletedAtMs: completedAt(900), SegmentStart: 12, SegmentEnd: 12,
				QueueWaitMs: 50, MeteredTimeMs: 150,
				ProveTimeMs: 200, StarkProveTimeMs: 200,
			},
		},
		LeafProofs: []taskTiming{{
			WorkerID: 5, DispatchedAtMs: completedAt(2150), WorkerStartMs: completedAt(2160), WorkerEndMs: completedAt(2480),
			CompletedAtMs: completedAt(2500), SegmentStart: 24, SegmentEnd: 27, ProveTimeMs: 300,
			SubMetrics: map[string]float64{"trace_gen_time_ms": 19.6, "prover.unlabelled_time_ms": 500},
		}},
		InternalProofs: []taskTiming{{
			DispatchedAtMs: completedAt(3100), WorkerStartMs: completedAt(3110), WorkerEndMs: completedAt(3990),
			CompletedAtMs: completedAt(4000), SegmentStart: 0, SegmentEnd: 229, LayerIndex: 2,
			ProveTimeMs: 600, CompressionTimeMs: 250,
			SubMetrics:     map[string]float64{"trace_gen_time_ms": 80},
			WrapSubMetrics: map[string]float64{"trace_gen_time_ms": 35, "prover.openings_time_ms": 90},
		}},
	}
	registrations := []workerRegistration{
		{WorkerID: 5, WorkerURL: "http://10.0.0.2:8002"},
		{WorkerID: 0, WorkerURL: "http://10.0.0.1:8001"},
	}

	pipeline, err := mapPipeline(&view, registrations)
	if err != nil {
		t.Fatal(err)
	}

	wantWorkers := []cluster.Worker{{Name: "worker_0", Node: "node1"}, {Name: "worker_5", Node: "node2"}}
	if !slices.Equal(pipeline.Workers, wantWorkers) {
		t.Errorf("workers = %+v, want %+v", pipeline.Workers, wantWorkers)
	}
	if !slices.Equal(pipeline.Breakdown, breakdownLabels) || pipeline.SchemaVersion != 2 {
		t.Errorf("pipeline = %+v", pipeline)
	}
	wantTasks := `[` +
		`[1,1,"#7",0,50,[]],` +
		`[0,0,"#24",40,300,[]],` +
		`[2,1,"#7",50,250,[[8,5]]],` +
		`[0,1,"#12",500,150,[]],` +
		`[1,0,"#24",540,40,[]],` +
		`[2,0,"#24",580,1120,[[0,30],[1,60],[2,40],[3,90],[4,270],[5,280],[6,50],[7,250],[8,20]]],` +
		`[2,1,"#12",700,200,[]],` +
		`[3,1,"24-27",2150,350,[[2,20],[8,30]]],` +
		`[4,0,"L2 0-229",3100,650,[[2,80],[8,20]]],` +
		`[5,0,"L2 0-229",3750,250,[[2,35],[7,90]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

// A leaf draws its bar over the whole round trip, from the dispatch to the
// receipt, and the two legs outside the worker stamps make the network
// transport. The worker window less the sections stays unattributed.
func TestPipelinePlacesALeafOverItsRoundTrip(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	stamp := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		LeafProofs: []taskTiming{{
			DispatchedAtMs: stamp(100), WorkerStartMs: stamp(130), WorkerEndMs: stamp(480), CompletedAtMs: stamp(500),
			SegmentEnd: 3, ProveTimeMs: 300, SubMetrics: map[string]float64{"trace_gen_time_ms": 200},
		}},
	}

	if tasks, want := placedTasks(t, view), `[[3,0,"0-3",100,400,[[2,200],[8,50]]]]`; tasks != want {
		t.Errorf("tasks = %s, want %s", tasks, want)
	}
}

// The first app segment of a worker stamps the task receipt, not its proving
// start, so its segment lasts the stark proving time plus both legs and keeps
// the unattributed time of the stark proving. The fast forward and the
// metering chain back from the segment start.
func TestPipelinePlacesTheFirstSegmentOverItsProvingAndTransport(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	stamp := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		AppProofs: []taskTiming{{
			DispatchedAtMs: stamp(100), WorkerStartMs: stamp(110), WorkerEndMs: stamp(1985), CompletedAtMs: stamp(2000),
			QueueWaitMs: 50, MeteredTimeMs: 300, FastForwardTimeMs: 100, StarkProveTimeMs: 1000,
		}},
	}

	want := `[[0,0,"#0",525,300,[]],[1,0,"#0",875,100,[]],[2,0,"#0",975,1025,[[8,25]]]]`
	if tasks := placedTasks(t, view); tasks != want {
		t.Errorf("tasks = %s, want %s", tasks, want)
	}
}

// A later app segment fast forwards from its worker start and proves until the
// receipt, and only the reply leg makes the network transport.
func TestPipelinePlacesALaterSegmentFromItsWorkerStart(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	stamp := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		AppProofs: []taskTiming{{
			WorkerStartMs: stamp(1000), WorkerEndMs: stamp(2180), CompletedAtMs: stamp(2200), SegmentStart: 5, SegmentEnd: 5,
			QueueWaitMs: 50, MeteredTimeMs: 300, FastForwardTimeMs: 100, StarkProveTimeMs: 1000,
		}},
	}

	want := `[[0,0,"#5",650,300,[]],[1,0,"#5",1000,100,[]],[2,0,"#5",1100,1100,[[8,20]]]]`
	if tasks := placedTasks(t, view); tasks != want {
		t.Errorf("tasks = %s, want %s", tasks, want)
	}
}

// An internal task starts at the dispatch and ends where its wrap starts, and
// the internal bar carries the network transport of both legs.
func TestPipelinePlacesAnInternalTaskFromItsDispatchToItsWrap(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	stamp := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		InternalProofs: []taskTiming{{
			DispatchedAtMs: stamp(100), WorkerStartMs: stamp(120), WorkerEndMs: stamp(990), CompletedAtMs: stamp(1000),
			SegmentEnd: 7, ProveTimeMs: 500, CompressionTimeMs: 300,
		}},
	}

	want := `[[4,0,"L0 0-7",100,600,[[8,30]]],[5,0,"L0 0-7",700,300,[]]]`
	if tasks := placedTasks(t, view); tasks != want {
		t.Errorf("tasks = %s, want %s", tasks, want)
	}
}

// A worker clock ahead of the manager clock stamps the reply after its receipt,
// and the negative network transport clamps to zero and is left out.
func TestPipelineClampsANegativeNetworkTransport(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	stamp := func(offsetMs int64) int64 { return proofStart.UnixMilli() + offsetMs }
	view := pipelineView{
		ProofStartTime: proofStart,
		AppProofs: []taskTiming{{
			WorkerStartMs: stamp(1000), WorkerEndMs: stamp(2230), CompletedAtMs: stamp(2200), SegmentStart: 5, SegmentEnd: 5,
			StarkProveTimeMs: 1000,
		}},
	}

	if tasks, want := placedTasks(t, view), `[[2,0,"#5",1000,1200,[]]]`; tasks != want {
		t.Errorf("tasks = %s, want %s", tasks, want)
	}
}

func TestPipelineRefusesAnUnlistedWorker(t *testing.T) {
	proofStart := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	view := pipelineView{
		ProofStartTime: proofStart,
		LeafProofs: []taskTiming{{
			WorkerID: 7, CompletedAtMs: proofStart.UnixMilli() + 900, SegmentEnd: 3, ProveTimeMs: 300,
		}},
	}

	_, err := mapPipeline(&view, []workerRegistration{{WorkerURL: "http://10.0.0.1:8001"}})
	if err == nil || !strings.Contains(err.Error(), "worker 7") {
		t.Errorf("err = %v, want the unlisted worker named", err)
	}
}

func TestWorkerRegistrationDecodesThePair(t *testing.T) {
	var workers workerList
	body := `{"num_workers":1,"expected_num_workers":1,"workers":[[5,{"worker_url":"http://10.0.0.2:8002","last_seen":"2026-09-03T10:00:00Z","worker_role":"full"}]]}`
	if err := json.Unmarshal([]byte(body), &workers); err != nil {
		t.Fatal(err)
	}
	if want := (workerRegistration{WorkerID: 5, WorkerURL: "http://10.0.0.2:8002"}); workers.Workers[0] != want {
		t.Errorf("registration = %+v, want %+v", workers.Workers[0], want)
	}
}
