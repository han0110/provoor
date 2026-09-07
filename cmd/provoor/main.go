// Command provoor deploys zkVM proving clusters and forwards benchmark
// requests to them.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/estimate"
	"github.com/han0110/provoor/internal/openvm"
	"github.com/han0110/provoor/internal/serve"
	"github.com/han0110/provoor/internal/zisk"
)

// version is stamped at build time with -ldflags "-X main.version=<tag>".
var version = "dev"

// backend is the deploy lifecycle every zkVM package provides.
type backend interface {
	Up(ctx context.Context, w io.Writer) error
	Down(ctx context.Context, w io.Writer) error
	Containers() []cluster.Deployed
}

func main() {
	root := &cobra.Command{
		Use:          "provoor",
		Short:        "Deploys zkVM proving clusters and forwards benchmark requests to them",
		Version:      version,
		SilenceUsage: true,
	}
	root.AddCommand(
		clusterCommand("up", "Deploys the proving cluster and blocks until it is ready", backend.Up),
		clusterCommand("down", "Stops and removes the proving cluster containers", backend.Down),
		psCommand(),
		logsCommand(),
		serveCommand(),
		estimateCommand(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Restoring the default disposition after the first signal lets a second
	// interrupt end a cleanup that does not return.
	go func() {
		<-ctx.Done()
		stop()
	}()

	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

func clusterCommand(use, short string, run func(backend, context.Context, io.Writer) error) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b, err := loadBackend(configPath)
			if err != nil {
				return err
			}
			return run(b, cmd.Context(), cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

func loadBackend(path string) (backend, error) {
	zkvm, err := cluster.Zkvm(path)
	if err != nil {
		return nil, err
	}
	switch zkvm {
	case "zisk":
		return zisk.Load(path)
	case "openvm":
		return openvm.Load(path)
	default:
		return nil, fmt.Errorf("zkvm %q is not supported, only zisk and openvm", zkvm)
	}
}

// selectContainers is the container selection a configuration deploys, nil
// when none is given, which selects every container the label marks instead.
func selectContainers(configPath string) ([]cluster.Deployed, error) {
	if configPath == "" {
		return nil, nil
	}
	b, err := loadBackend(configPath)
	if err != nil {
		return nil, err
	}
	return b.Containers(), nil
}

// dialSelection dials the hosts a selection names in configuration order, or
// the local daemon when no configuration selects them.
func dialSelection(ctx context.Context, selection []cluster.Deployed) (*cluster.Hosts, error) {
	destinations := []string{""}
	if len(selection) > 0 {
		destinations = make([]string, len(selection))
		for i, deployed := range selection {
			destinations[i] = deployed.SSH
		}
	}
	return cluster.DialHosts(ctx, destinations)
}

func psCommand() *cobra.Command {
	var (
		configPath string
		all        bool
	)
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "Lists the containers provoor deployed",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			selection, err := selectContainers(configPath)
			if err != nil {
				return err
			}
			hosts, err := dialSelection(cmd.Context(), selection)
			if err != nil {
				return err
			}
			defer hosts.Close()
			listed, err := cluster.List(cmd.Context(), hosts, selection, all)
			if err != nil {
				return err
			}
			return cluster.WriteTable(cmd.OutOrStdout(), listed)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file, by default the local daemon")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "list the stopped containers too")
	return cmd
}

func logsCommand() *cobra.Command {
	var (
		configPath string
		follow     bool
		timestamps bool
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Prints the logs of the deployed cluster containers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			selection, err := selectContainers(configPath)
			if err != nil {
				return err
			}
			hosts, err := dialSelection(cmd.Context(), selection)
			if err != nil {
				return err
			}
			defer hosts.Close()
			// A stopped container still holds the log of its last run.
			listed, err := cluster.List(cmd.Context(), hosts, selection, true)
			if err != nil {
				return err
			}
			return cluster.StreamLogs(cmd.Context(), hosts, listed, follow, timestamps, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file, by default the local daemon")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep streaming until interrupted")
	cmd.Flags().BoolVarP(&timestamps, "timestamps", "t", false, "print the timestamp of every line")
	cmd.AddCommand(logsDumpCommand())
	return cmd
}

func logsDumpCommand() *cobra.Command {
	var (
		runDir     string
		configPath string
		sudo       bool
	)
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Writes the journal of the coordinator and every worker for a run's time window into the run directory",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b, err := loadBackend(configPath)
			if err != nil {
				return err
			}
			since, until, err := cluster.RunWindow(runDir)
			if err != nil {
				return err
			}
			return cluster.DumpJournals(cmd.Context(), b.Containers(), runDir, since, until, sudo, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&runDir, "run", "", "benchmark run directory, which names the time window")
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file")
	cmd.Flags().BoolVar(&sudo, "sudo", false, "run journalctl under sudo -n on the remote hosts")
	for _, flag := range []string{"run", "config"} {
		_ = cmd.MarkFlagRequired(flag)
	}
	return cmd
}

func serveCommand() *cobra.Command {
	var (
		zkvm                string
		statelessValidator  string
		elfSource           string
		verifyingKeySource  string
		coordinatorEndpoint string
		listen              string
		timeout             time.Duration
		onClusterError      string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Forwards benchmark requests to the proving cluster over JSON-RPC",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if zkvm != "zisk" && zkvm != "openvm" {
				return fmt.Errorf("zkvm %q is not supported, only zisk and openvm", zkvm)
			}
			if onClusterError != "fail-test" && onClusterError != "fail-run" {
				return fmt.Errorf("on-cluster-error %q is not fail-test or fail-run", onClusterError)
			}
			ctx, out := cmd.Context(), cmd.OutOrStdout()

			elf, err := cluster.ResolveSource(ctx, elfSource)
			if err != nil {
				return err
			}
			programVK, err := cluster.ResolveSource(ctx, verifyingKeySource)
			if err != nil {
				return err
			}
			// The dial provisions the guest on the cluster, and the readiness
			// wait covers a cluster that cannot take work yet.
			var prover serve.Prover
			var provisioned string
			switch zkvm {
			case "zisk":
				client, err := zisk.Dial(ctx, coordinatorEndpoint, elf, programVK)
				if err != nil {
					return err
				}
				defer func() { _ = client.Close() }()
				prover, provisioned = client, fmt.Sprintf("registered, hash %s", client.HashID)
			case "openvm":
				client, err := openvm.Dial(ctx, coordinatorEndpoint, elf, programVK)
				if err != nil {
					return err
				}
				defer func() { _ = client.Close() }()
				prover, provisioned = client, fmt.Sprintf("provisioned, program %s", client.ProgramName)
			}
			if err := prover.WaitReady(ctx); err != nil {
				return err
			}
			fmt.Fprintf(out, "stateless validator %s %s\n", statelessValidator, provisioned)

			server := &serve.Server{
				Prover:                prover,
				ClientVersion:         cluster.GuestELFName(elfSource),
				ProveTimeout:          timeout,
				FailRunOnClusterError: onClusterError == "fail-run",
				Output:                out,
				Exit:                  os.Exit,
			}
			warmupStart := time.Now()
			if err := server.Warmup(ctx); err != nil {
				return err
			}
			fmt.Fprintf(out, "prover warmed in %s\n", time.Since(warmupStart).Round(time.Second))

			// The port opens only after verification, setup, and warmup, so a
			// readiness poll cannot race the first proof.
			listener, err := net.Listen("tcp", listen)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "listening on %s\n", listener.Addr())
			httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpServer.Shutdown(shutdownCtx)
			}()
			if err := httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&zkvm, "zkvm", "", "proving backend, zisk or openvm")
	cmd.Flags().StringVar(&statelessValidator, "stateless-validator", "", "stateless validator name, for example ethrex")
	cmd.Flags().StringVar(&elfSource, "elf", "", "guest ELF source, a local path or an http(s) URL")
	cmd.Flags().StringVar(&verifyingKeySource, "vk", "", "guest verifying key source, a local path or an http(s) URL")
	cmd.Flags().StringVar(&coordinatorEndpoint, "coordinator-endpoint", "", "coordinator API endpoint, for example http://10.0.0.1:7000")
	cmd.Flags().StringVar(&listen, "listen", ":8551", "listen address")
	cmd.Flags().DurationVar(&timeout, "timeout", cluster.DefaultProveTimeout, "per-proof timeout")
	cmd.Flags().StringVar(&onClusterError, "on-cluster-error", "fail-test", "fail-test answers an error and continues, fail-run exits")
	for _, flag := range []string{"zkvm", "stateless-validator", "elf", "vk", "coordinator-endpoint"} {
		_ = cmd.MarkFlagRequired(flag)
	}
	return cmd
}

func estimateCommand() *cobra.Command {
	var (
		image       string
		concurrency int
	)
	cmd := &cobra.Command{
		Use:   "estimate <run-dir>",
		Short: "Estimates the proving cost of every test of a benchmark run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if concurrency < 1 {
				return fmt.Errorf("concurrency %d is not a positive count", concurrency)
			}
			return estimate.Run(cmd.Context(), args[0], image, concurrency, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&image, "image", "", "ere-server image, by default the one of the run's zkVM")
	cmd.Flags().IntVarP(&concurrency, "concurrency", "c", min(16, runtime.NumCPU()), "concurrent estimations")
	return cmd
}
