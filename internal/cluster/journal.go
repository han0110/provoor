package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// journalTimeFormat is the fixed width stamp docker logs prints with
// timestamps.
const journalTimeFormat = "2006-01-02T15:04:05.000000000Z07:00"

// journalFields are the fields a dump reads, the message and the marker the
// journald driver sets on every part but the last of a long line.
const journalFields = "MESSAGE,CONTAINER_PARTIAL_MESSAGE"

// journalLineLimit bounds one json entry, which carries one message.
const journalLineLimit = 1 << 20

// RunWindow is the time a benchmark run covers, read from its config.json. A
// run that carries no end is unfinished, so its window reaches the present.
func RunWindow(dir string) (time.Time, time.Time, error) {
	path := filepath.Join(dir, "config.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	var parsed struct {
		Timestamp    int64 `json:"timestamp"`
		TimestampEnd int64 `json:"timestamp_end"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if parsed.Timestamp == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("%s carries no timestamp", path)
	}
	until := time.Now()
	if parsed.TimestampEnd != 0 {
		until = time.Unix(parsed.TimestampEnd, 0)
	}
	return time.Unix(parsed.Timestamp, 0), until, nil
}

// DumpJournals writes the journal of every coordinator and worker of a
// selection into dir over a time window, one file per container named after
// its label. An existing file is overwritten and the containers are read
// in parallel. Every container is attempted and every failure is reported.
// What journalctl reports on its error stream prints behind the label. With
// sudo set, journalctl runs under sudo -n on every remote host.
func DumpJournals(ctx context.Context, selection []Deployed, dir string, since, until time.Time, sudo bool, w io.Writer) error {
	out := NewOutput(w)
	errs := make([]error, len(selection))
	var wg sync.WaitGroup
	for i, deployed := range selection {
		if deployed.Sidecar {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = dumpJournal(ctx, deployed, dir, since, until, sudo, out)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// dumpJournal writes one container's journal into its own file. A failed read
// leaves no file behind.
func dumpJournal(ctx context.Context, deployed Deployed, dir string, since, until time.Time, sudo bool, out *Output) (err error) {
	name := deployed.Label + ".log"
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()

	command, args := journalCommand(deployed.SSH, deployed.Name, since, until, sudo)
	cmd := exec.CommandContext(ctx, command, args...)
	reported := out.Prefixed(deployed.Label)
	cmd.Stderr = reported
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("reading the journal of %s on %s: %w", deployed.Name, HostName(deployed.SSH), err)
	}
	lines, writeErr := writeJournal(stdout, file)
	// A decoder that stopped early leaves the command blocked on the pipe.
	_, _ = io.Copy(io.Discard, stdout)
	waitErr := cmd.Wait()
	reported.Flush()
	if waitErr != nil {
		return fmt.Errorf("reading the journal of %s on %s: %w", deployed.Name, HostName(deployed.SSH), waitErr)
	}
	if writeErr != nil {
		return fmt.Errorf("writing %s: %w", name, writeErr)
	}
	out.Printf("[%s] %s, %d lines", deployed.Label, name, lines)
	return nil
}

// journalCommand prints one container's journal over a window, run locally
// for an empty destination and over the local ssh binary otherwise. With sudo
// set, a remote journal is read under sudo -n, which needs passwordless sudo
// for the SSH user. The json output nulls a message over 4 KiB unless --all is
// given, and --quiet keeps the informational lines off the stream.
func journalCommand(destination, name string, since, until time.Time, sudo bool) (string, []string) {
	args := []string{
		"journalctl", "--quiet", "--all", "-o", "json", "--utc", "--no-pager",
		"--output-fields=" + journalFields,
		"CONTAINER_NAME=" + name,
		"--since", fmt.Sprintf("@%d", since.Unix()),
		"--until", fmt.Sprintf("@%d", until.Unix()),
	}
	if destination == "" {
		return args[0], args[1:]
	}
	if sudo {
		args = append([]string{"sudo", "-n"}, args...)
	}
	return "ssh", append(sshArgs(destination), args...)
}

// sshArgs maps an SSH destination onto the arguments the local ssh binary
// takes, with an explicit port passed as an option.
func sshArgs(destination string) []string {
	target := strings.TrimPrefix(destination, "ssh://")
	at := strings.LastIndex(target, "@")
	host := target[at+1:]
	colon := strings.LastIndex(host, ":")
	if colon < 0 {
		return []string{target}
	}
	return []string{"-p", host[colon+1:], target[:at+1] + host[:colon]}
}

// writeJournal decodes the json entries of a journal into one line per
// message, as docker prints a log with timestamps, and reports how many lines
// it wrote. A line the journald driver split over entries joins back under
// the timestamp of its first part.
func writeJournal(r io.Reader, w io.Writer) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(nil, journalLineLimit)
	lines := 0
	var pending strings.Builder
	var start time.Time
	flush := func() error {
		if _, err := fmt.Fprintf(w, "%s %s\n", start.Format(journalTimeFormat), pending.String()); err != nil {
			return err
		}
		pending.Reset()
		lines++
		return nil
	}
	for scanner.Scan() {
		var entry struct {
			Realtime string          `json:"__REALTIME_TIMESTAMP"`
			Message  json.RawMessage `json:"MESSAGE"`
			Partial  string          `json:"CONTAINER_PARTIAL_MESSAGE"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return lines, fmt.Errorf("parsing the journal entry %s: %w", scanner.Bytes(), err)
		}
		message, err := journalMessage(entry.Message)
		if err != nil {
			return lines, err
		}
		if pending.Len() == 0 {
			if start, err = journalTime(entry.Realtime); err != nil {
				return lines, err
			}
		}
		pending.WriteString(message)
		if entry.Partial == "true" {
			continue
		}
		if err := flush(); err != nil {
			return lines, err
		}
	}
	if err := scanner.Err(); err != nil {
		return lines, err
	}
	// A journal cut off mid line still carries the part it holds.
	if pending.Len() > 0 {
		return lines, flush()
	}
	return lines, nil
}

// journalMessage decodes a MESSAGE field, a string for a valid UTF-8 line and
// an array of byte values for every other one. An entry without one is an
// empty line.
func journalMessage(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var values []int
	if err := json.Unmarshal(raw, &values); err != nil {
		return "", fmt.Errorf("parsing the journal message %s: %w", raw, err)
	}
	line := make([]byte, len(values))
	for i, value := range values {
		line[i] = byte(value)
	}
	return string(line), nil
}

// journalTime reads an entry's realtime stamp, microseconds since the epoch.
func journalTime(realtime string) (time.Time, error) {
	microseconds, err := strconv.ParseInt(realtime, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing the journal timestamp %q: %w", realtime, err)
	}
	return time.UnixMicro(microseconds).UTC(), nil
}
