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
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/han0110/provoor/internal/cluster"
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
		serveCommand(),
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
