package cluster

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWriteJournal(t *testing.T) {
	entries := []string{
		`{"__REALTIME_TIMESTAMP":"1788681922123456","MESSAGE":"a plain line"}`,
		// A line that is not valid UTF-8 arrives as an array of byte values.
		`{"__REALTIME_TIMESTAMP":"1788681922200000","MESSAGE":[104,105,255]}`,
		// The journald driver splits a long line over entries and marks every
		// part but the last.
		`{"__REALTIME_TIMESTAMP":"1788681922300000","MESSAGE":"first half ","CONTAINER_PARTIAL_MESSAGE":"true"}`,
		`{"__REALTIME_TIMESTAMP":"1788681922350000","MESSAGE":"second half ","CONTAINER_PARTIAL_MESSAGE":"true"}`,
		`{"__REALTIME_TIMESTAMP":"1788681922400000","MESSAGE":"and the rest"}`,
		// An entry without a message is an empty line.
		`{"__REALTIME_TIMESTAMP":"1788681922500000"}`,
	}

	var buf bytes.Buffer
	lines, err := writeJournal(strings.NewReader(strings.Join(entries, "\n")+"\n"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if lines != 4 {
		t.Errorf("lines = %d, want 4", lines)
	}
	want := "2026-09-06T08:05:22.123456000Z a plain line\n" +
		"2026-09-06T08:05:22.200000000Z hi\xff\n" +
		"2026-09-06T08:05:22.300000000Z first half second half and the rest\n" +
		"2026-09-06T08:05:22.500000000Z \n"
	if buf.String() != want {
		t.Errorf("journal = %q, want %q", buf.String(), want)
	}
}

// TestWriteJournalFlushesAPartialTail covers a journal whose last line never
// got its terminating entry, which would otherwise drop the line.
func TestWriteJournalFlushesAPartialTail(t *testing.T) {
	entry := `{"__REALTIME_TIMESTAMP":"1788681922123456","MESSAGE":"cut off","CONTAINER_PARTIAL_MESSAGE":"true"}`
	var buf bytes.Buffer
	lines, err := writeJournal(strings.NewReader(entry+"\n"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if want := "2026-09-06T08:05:22.123456000Z cut off\n"; lines != 1 || buf.String() != want {
		t.Errorf("journal = %d lines %q, want 1 line %q", lines, buf.String(), want)
	}
}

func TestJournalCommand(t *testing.T) {
	since := time.Unix(1788681922, 0)
	until := time.Unix(1788685522, 0)
	wantArgs := []string{
		"journalctl", "--quiet", "--all", "-o", "json", "--utc", "--no-pager",
		"--output-fields=MESSAGE,CONTAINER_PARTIAL_MESSAGE",
		"CONTAINER_NAME=zisk-worker",
		"--since", "@1788681922",
		"--until", "@1788685522",
	}
	wantSudo := append([]string{"sudo", "-n"}, wantArgs...)

	// The local journal is read as the user, whatever sudo says.
	for _, sudo := range []bool{false, true} {
		name, args := journalCommand("", "zisk-worker", since, until, sudo)
		if name != "journalctl" || !slices.Equal(args, wantArgs[1:]) {
			t.Errorf("local command with sudo %t = %s %v, want journalctl %v", sudo, name, args, wantArgs[1:])
		}
	}
	for destination, wantSSH := range map[string][]string{
		"node1":                    {"node1"},
		"user@10.0.0.1":            {"user@10.0.0.1"},
		"ssh://user@10.0.0.1":      {"user@10.0.0.1"},
		"ssh://user@10.0.0.1:2222": {"-p", "2222", "user@10.0.0.1"},
		"ssh://10.0.0.1:2222":      {"-p", "2222", "10.0.0.1"},
	} {
		name, args := journalCommand(destination, "zisk-worker", since, until, false)
		if name != "ssh" || !slices.Equal(args, append(wantSSH, wantArgs...)) {
			t.Errorf("command for %q = %s %v, want ssh %v", destination, name, args, append(wantSSH, wantArgs...))
		}
		name, args = journalCommand(destination, "zisk-worker", since, until, true)
		if name != "ssh" || !slices.Equal(args, append(wantSSH, wantSudo...)) {
			t.Errorf("sudo command for %q = %s %v, want ssh %v", destination, name, args, append(wantSSH, wantSudo...))
		}
	}
}

func TestRunWindow(t *testing.T) {
	dir := t.TempDir()
	write := func(config string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(config), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write(`{"timestamp":1788681922,"timestamp_end":1788685522}`)
	since, until, err := RunWindow(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !since.Equal(time.Unix(1788681922, 0)) || !until.Equal(time.Unix(1788685522, 0)) {
		t.Errorf("window = %s to %s, want the recorded timestamps", since, until)
	}

	// An unfinished run carries no end, so the window reaches the present.
	write(`{"timestamp":1788681922}`)
	since, until, err = RunWindow(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !since.Equal(time.Unix(1788681922, 0)) || time.Since(until) > time.Minute {
		t.Errorf("window = %s to %s, want it to end now", since, until)
	}

	write(`{"suite_hash":"dfad8a20"}`)
	if _, _, err := RunWindow(dir); err == nil {
		t.Error("a run without a timestamp is accepted")
	}
}

// TestDumpJournalsReportsEveryContainer covers a dump that fails on every
// container, which reports one failure per container and leaves the sidecars
// out.
func TestDumpJournalsReportsEveryContainer(t *testing.T) {
	selection := []Deployed{
		{Name: "zisk-coordinator", Label: CoordinatorName},
		{Name: "zisk-worker", Label: "worker_0-gpu_0"},
		{Name: "provoor-node-local", Label: "dcgm-exporter", Sidecar: true},
	}
	// A directory that does not exist fails every container before it reads a
	// journal.
	dir := filepath.Join(t.TempDir(), "missing")

	var out bytes.Buffer
	err := DumpJournals(t.Context(), selection, dir, time.Unix(1788681922, 0), time.Unix(1788685522, 0), false, &out)
	if err == nil {
		t.Fatal("a dump into a missing directory is accepted")
	}
	for _, label := range []string{CoordinatorName, "worker_0-gpu_0"} {
		if !strings.Contains(err.Error(), label+".log") {
			t.Errorf("err = %v, want it to name %s", err, label)
		}
	}
	if strings.Contains(err.Error(), "dcgm-exporter") {
		t.Error("the dump reads the journal of a sidecar")
	}
}

// TestDumpJournals runs a journalctl stand-in that answers the coordinator
// with two entries and fails the worker, whose file must not remain.
func TestDumpJournals(t *testing.T) {
	binDir := t.TempDir()
	script := `#!/bin/sh
case "$*" in
*CONTAINER_NAME=zisk-coordinator*)
	echo '{"__REALTIME_TIMESTAMP":"1788681922123456","MESSAGE":"first"}'
	echo '{"__REALTIME_TIMESTAMP":"1788681923000000","MESSAGE":"second"}'
	;;
*)
	echo 'no journal for you' >&2
	exit 1
	;;
esac
`
	if err := os.WriteFile(filepath.Join(binDir, "journalctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	dir := t.TempDir()
	selection := []Deployed{
		{Name: "zisk-coordinator", Label: CoordinatorName},
		{Name: "zisk-worker", Label: "worker_0-gpu_0"},
	}

	var out bytes.Buffer
	err := DumpJournals(t.Context(), selection, dir, time.Unix(1788681922, 0), time.Unix(1788685522, 0), false, &out)
	if err == nil || !strings.Contains(err.Error(), "zisk-worker") {
		t.Fatalf("err = %v, want the worker failure", err)
	}
	journal, err := os.ReadFile(filepath.Join(dir, "coordinator.log"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "2026-09-06T08:05:22.123456000Z first\n2026-09-06T08:05:23.000000000Z second\n"; string(journal) != want {
		t.Errorf("coordinator.log = %q, want %q", journal, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "worker_0-gpu_0.log")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("worker_0-gpu_0.log stat = %v, want it absent after the failure", err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	slices.Sort(lines)
	if want := []string{"[coordinator] coordinator.log, 2 lines", "[worker_0-gpu_0] no journal for you"}; !slices.Equal(lines, want) {
		t.Errorf("out =\n%v\nwant\n%v", lines, want)
	}
}
