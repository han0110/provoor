package cluster

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
)

// PipelineSchemaVersion is the wire version of the timeline.
const PipelineSchemaVersion = 1

// Phases a task belongs to. The frontend colors by phase and shades by legend
// entry, and titles every bar by kind.
const (
	PhaseExecution = "execution"
	PhaseWitness   = "witness"
	PhaseBase      = "base"
	PhaseRecursion = "recursion"
	PhaseWrap      = "wrap"
)

// Pipeline is the task timeline of one proof, the same shape for every zkVM.
type Pipeline struct {
	SchemaVersion int        `json:"schemaVersion"`
	Kinds         []TaskKind `json:"kinds"`
	Breakdown     []string   `json:"breakdown"`
	Workers       []Worker   `json:"workers"`
	Tasks         []Task     `json:"tasks"`
}

// TaskKind is one stage of proving, such as a segment or a leaf aggregation.
// Label titles the bars of the kind, Legend names the legend entry the kind
// draws under, and Row names the row group it draws on. Kinds of one legend
// entry share one shade, so they share a phase. The tasks of one worker, one
// row group, and one id share a row.
type TaskKind struct {
	Name   string `json:"name"`
	Label  string `json:"label"`
	Legend string `json:"legend"`
	Row    string `json:"row"`
	Phase  string `json:"phase"`
}

// Worker is one prover process and the node it runs on.
type Worker struct {
	Name string `json:"name"`
	Node string `json:"node"`
}

// Task is one unit of work a worker ran. Kind and Worker index the lists of
// the pipeline, and the times are milliseconds from the proof start.
type Task struct {
	Kind       int
	Worker     int
	ID         string
	StartMs    int64
	DurationMs int64
	// Breakdown pairs an index into Pipeline.Breakdown with milliseconds.
	Breakdown [][2]int64
}

// MarshalJSON encodes the task as the row [kind, worker, id, startMs,
// durationMs, breakdown], which keeps a timeline of thousands of tasks small.
func (t Task) MarshalJSON() ([]byte, error) {
	breakdown := t.Breakdown
	if breakdown == nil {
		breakdown = [][2]int64{}
	}
	return json.Marshal([]any{t.Kind, t.Worker, t.ID, t.StartMs, t.DurationMs, breakdown})
}

// Registration is one worker the coordinator knows. Key is what a task result
// names its worker by, Name is what the timeline shows, and Host groups the
// workers into nodes.
type Registration[Key comparable] struct {
	Key  Key
	Name string
	Host string
}

// PipelineBuilder places the task results of one proof on the proof clock.
type PipelineBuilder[Key comparable] struct {
	workers     []Worker
	workerIndex map[Key]int
	tasks       []Task
	unlisted    error
}

// NewPipelineBuilder names every registered worker and groups the workers into
// nodes. Nodes are node1 and up in the order the registrations first name each
// host.
func NewPipelineBuilder[Key comparable](registrations []Registration[Key]) *PipelineBuilder[Key] {
	builder := &PipelineBuilder[Key]{
		workers:     make([]Worker, 0, len(registrations)),
		workerIndex: make(map[Key]int, len(registrations)),
		tasks:       []Task{},
	}
	nodes := make(map[string]string, len(registrations))
	for _, registration := range registrations {
		node, named := nodes[registration.Host]
		if !named {
			node = fmt.Sprintf("node%d", len(nodes)+1)
			nodes[registration.Host] = node
		}
		builder.workerIndex[registration.Key] = len(builder.workers)
		builder.workers = append(builder.workers, Worker{Name: registration.Name, Node: node})
	}
	return builder
}

// Place records one task of worker ending endMs after the proof start and
// lasting durationMs, and returns its start. A start before the proof start
// clamps to zero, so the task keeps its end and shrinks, and a task of no
// length or a negative length is left out and reports its end. A worker no
// registration names fails Pipeline.
func (builder *PipelineBuilder[Key]) Place(kind int, worker Key, id string, endMs, durationMs int64, breakdown [][2]int64) int64 {
	index, listed := builder.workerIndex[worker]
	if !listed && builder.unlisted == nil {
		builder.unlisted = fmt.Errorf("task of worker %v, which the coordinator does not list", worker)
	}
	startMs, endMs := max(endMs-durationMs, 0), max(endMs, 0)
	if startMs >= endMs {
		return endMs
	}
	builder.tasks = append(builder.tasks, Task{
		Kind:       kind,
		Worker:     index,
		ID:         id,
		StartMs:    startMs,
		DurationMs: endMs - startMs,
		Breakdown:  breakdown,
	})
	return startMs
}

// Pipeline sorts the tasks by start, then by kind, and returns the timeline.
// Tasks and Workers are always arrays, because the frontend walks both with
// no guard.
func (builder *PipelineBuilder[Key]) Pipeline(kinds []TaskKind, breakdown []string) (*Pipeline, error) {
	if builder.unlisted != nil {
		return nil, builder.unlisted
	}
	slices.SortStableFunc(builder.tasks, func(a, b Task) int {
		return cmp.Or(cmp.Compare(a.StartMs, b.StartMs), cmp.Compare(a.Kind, b.Kind))
	})
	return &Pipeline{
		SchemaVersion: PipelineSchemaVersion,
		Kinds:         kinds,
		Breakdown:     breakdown,
		Workers:       builder.workers,
		Tasks:         builder.tasks,
	}, nil
}
