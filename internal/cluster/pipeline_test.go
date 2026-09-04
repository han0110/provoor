package cluster

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestTaskMarshalsAsARow(t *testing.T) {
	cases := map[string]Task{
		`[2,0,"24",1015,1100,[[0,30]]]`: {Kind: 2, ID: "24", StartMs: 1015, DurationMs: 1100, Breakdown: [][2]int64{{0, 30}}},
		`[0,1,"",0,480,[]]`:             {Worker: 1, DurationMs: 480},
	}
	for want, task := range cases {
		row, err := json.Marshal(task)
		if err != nil || string(row) != want {
			t.Errorf("row = %s, %v, want %s", row, err, want)
		}
	}
}

// The registrations name two workers on one host and a third on another, so
// the nodes follow the order of first sight. One task reaches back before the
// proof start and keeps its end, one shares a start with another and sorts by
// kind, and the two of no length drop and report the end a chained task ends
// at.
func TestPipelineBuilderPlacesTasks(t *testing.T) {
	builder := NewPipelineBuilder([]Registration[string]{
		{Key: "a", Name: "worker_a", Host: "host1"},
		{Key: "b", Name: "worker_b", Host: "host2"},
		{Key: "c", Name: "worker_c", Host: "host1"},
	})
	if start := builder.Place(1, "b", "7", 300, 500, nil); start != 0 {
		t.Errorf("start = %d, want 0", start)
	}
	builder.Place(0, "b", "x", 300, 300, nil)
	builder.Place(0, "a", "", 1000, 400, [][2]int64{{0, 40}})
	for _, durationMs := range []int64{0, -100} {
		if end := builder.Place(2, "c", "9", 800, durationMs, nil); end != 800 {
			t.Errorf("end = %d, want 800", end)
		}
	}

	pipeline, err := builder.Pipeline([]TaskKind{{Name: "execution"}}, []string{"Emulation"})
	if err != nil {
		t.Fatal(err)
	}
	wantWorkers := []Worker{{Name: "worker_a", Node: "node1"}, {Name: "worker_b", Node: "node2"}, {Name: "worker_c", Node: "node1"}}
	if !slices.Equal(pipeline.Workers, wantWorkers) || pipeline.SchemaVersion != PipelineSchemaVersion {
		t.Errorf("pipeline = %+v, want workers %+v", pipeline, wantWorkers)
	}
	wantTasks := `[[0,1,"x",0,300,[]],[1,1,"7",0,300,[]],[0,0,"",600,400,[[0,40]]]]`
	tasks, err := json.Marshal(pipeline.Tasks)
	if err != nil || string(tasks) != wantTasks {
		t.Errorf("tasks = %s, %v, want %s", tasks, err, wantTasks)
	}
}

func TestPipelineBuilderRefusesAnUnlistedWorker(t *testing.T) {
	builder := NewPipelineBuilder([]Registration[int]{{Key: 0, Name: "worker_0", Host: "host1"}})
	builder.Place(0, 7, "", 900, 300, nil)

	if _, err := builder.Pipeline(nil, nil); err == nil || !strings.Contains(err.Error(), "worker 7") {
		t.Errorf("err = %v, want the unlisted worker named", err)
	}
}

func TestPipelineEncodesEmptyLists(t *testing.T) {
	pipeline, err := NewPipelineBuilder[int](nil).Pipeline(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(pipeline)
	if err != nil || !strings.Contains(string(encoded), `"workers":[],"tasks":[]`) {
		t.Errorf("pipeline = %s, %v, want empty arrays", encoded, err)
	}
}
