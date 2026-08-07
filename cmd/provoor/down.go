package main

import (
	"github.com/spf13/cobra"
)

func downCommand() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "down",
		Short: "Stops and removes the proving cluster containers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := loadCluster(configPath)
			if err != nil {
				return err
			}
			return c.Down(cmd.Context(), cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
