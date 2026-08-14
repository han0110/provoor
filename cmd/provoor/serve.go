package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/cluster/openvm"
	"github.com/han0110/provoor/pkg/cluster/zisk"
	"github.com/han0110/provoor/pkg/serve"
)

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

			ctx := cmd.Context()
			elf, err := cluster.ResolveSource(ctx, elfSource)
			if err != nil {
				return err
			}
			programVK, err := cluster.ResolveSource(ctx, verifyingKeySource)
			if err != nil {
				return err
			}
			var prover serve.Prover
			switch zkvm {
			case "zisk":
				ziskProver, err := zisk.NewProver(ctx, coordinatorEndpoint, elf, programVK, elfSource)
				if err != nil {
					return err
				}
				defer func() { _ = ziskProver.Close() }()
				fmt.Fprintf(cmd.OutOrStdout(), "stateless validator %s registered, hash %s\n", statelessValidator, ziskProver.HashID())
				prover = ziskProver
			case "openvm":
				openvmProver, err := openvm.NewProver(ctx, coordinatorEndpoint, elf, programVK, elfSource)
				if err != nil {
					return err
				}
				defer func() { _ = openvmProver.Close() }()
				fmt.Fprintf(cmd.OutOrStdout(), "stateless validator %s provisioned, program %s\n", statelessValidator, openvmProver.ProgramName())
				prover = openvmProver
			}

			// Resolved once so warmup and each proof share one budget, since
			// zero selects the default rather than an expired deadline.
			if timeout <= 0 {
				timeout = serve.DefaultProveTimeout
			}

			server := &serve.Server{
				Prover:                prover,
				ProveTimeout:          timeout,
				FailRunOnClusterError: onClusterError == "fail-run",
				Output:                cmd.OutOrStdout(),
				Exit:                  os.Exit,
			}

			// A warmup proof drives a cold cluster through its one-time
			// compile and cache costs, so no measured proof pays them.
			warmupStart := time.Now()
			warmupCtx, cancelWarmup := context.WithTimeout(ctx, timeout)
			err = prover.Warmup(warmupCtx)
			cancelWarmup()
			if err != nil {
				return fmt.Errorf("warming up the prover: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "prover warmed in %s\n", time.Since(warmupStart).Round(time.Second))

			// The listen port opens only after guest verification, setup, and
			// warmup, so a readiness poll cannot race the first proof.
			listener, err := net.Listen("tcp", listen)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "listening on %s\n", listener.Addr())

			httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpServer.Shutdown(shutdownCtx)
			}()
			if err := httpServer.Serve(listener); err != http.ErrServerClosed {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&zkvm, "zkvm", "", "proving backend, zisk or openvm")
	cmd.Flags().StringVar(&statelessValidator, "stateless-validator", "", "stateless validator name, for example ethrex")
	cmd.Flags().StringVar(&elfSource, "elf", "", "guest ELF source, a local path or an ere-guests release asset URL")
	cmd.Flags().StringVar(&verifyingKeySource, "vk", "", "guest verifying key source, a local path or an ere-guests release asset URL")
	cmd.Flags().StringVar(&coordinatorEndpoint, "coordinator-endpoint", "", "coordinator API endpoint, for example http://10.0.0.1:7000")
	cmd.Flags().StringVar(&listen, "listen", ":8551", "listen address")
	cmd.Flags().DurationVar(&timeout, "timeout", serve.DefaultProveTimeout, "per-proof timeout")
	cmd.Flags().StringVar(&onClusterError, "on-cluster-error", "fail-test", "fail-test answers an error and continues, fail-run exits")
	_ = cmd.MarkFlagRequired("zkvm")
	_ = cmd.MarkFlagRequired("stateless-validator")
	_ = cmd.MarkFlagRequired("elf")
	_ = cmd.MarkFlagRequired("vk")
	_ = cmd.MarkFlagRequired("coordinator-endpoint")
	return cmd
}
