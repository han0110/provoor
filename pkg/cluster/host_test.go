package cluster

import (
	"slices"
	"testing"
)

func TestDaemonURL(t *testing.T) {
	cases := []struct {
		destination string
		want        string
	}{
		{"", ""},
		{"node1", "ssh://node1"},
		{"user@203.0.113.1", "ssh://user@203.0.113.1"},
		{"ssh://user@203.0.113.1:2222", "ssh://user@203.0.113.1:2222"},
		{"unix:///var/run/docker.sock", "unix:///var/run/docker.sock"},
		{"tcp://203.0.113.1:2375", "tcp://203.0.113.1:2375"},
	}
	for _, tc := range cases {
		if got := daemonURL(tc.destination); got != tc.want {
			t.Errorf("daemonURL(%q) = %q, want %q", tc.destination, got, tc.want)
		}
	}
}

func TestHostName(t *testing.T) {
	cases := []struct {
		destination string
		want        string
	}{
		{"", "local"},
		{"node1", "node1"},
		{"user@203.0.113.1", "203.0.113.1"},
		{"ssh://user@203.0.113.1:2222", "203.0.113.1"},
	}
	for _, tc := range cases {
		if got := HostName(tc.destination); got != tc.want {
			t.Errorf("HostName(%q) = %q, want %q", tc.destination, got, tc.want)
		}
	}
}

// TestTunnelDialer pins the ssh invocation the tunnel spawns, since a wrong
// flag order or a dropped user silently falls back to the wrong account.
func TestTunnelDialer(t *testing.T) {
	// A local destination dials directly, so no tunnel is built.
	dialer, err := TunnelDialer("", "127.0.0.1:7000")
	if err != nil {
		t.Fatal(err)
	}
	if dialer != nil {
		t.Error("an empty destination should yield no dialer")
	}

	cases := []struct {
		destination string
		want        []string
	}{
		{"node1", []string{"-o", "ConnectTimeout=10", "-W", "127.0.0.1:7000", "--", "node1"}},
		{"user@node1", []string{"-l", "user", "-o", "ConnectTimeout=10", "-W", "127.0.0.1:7000", "--", "node1"}},
		{
			"ssh://user@node1:2222",
			[]string{"-l", "user", "-p", "2222", "-o", "ConnectTimeout=10", "-W", "127.0.0.1:7000", "--", "node1"},
		},
	}
	for _, tc := range cases {
		got, err := tunnelArgs(tc.destination, "127.0.0.1:7000")
		if err != nil {
			t.Fatalf("%s: %v", tc.destination, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s args = %v, want %v", tc.destination, got, tc.want)
		}
	}
}
