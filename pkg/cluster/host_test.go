package cluster

import "testing"

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
