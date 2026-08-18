package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func logsCommand(service Service) *cobra.Command {
	var project projectOptions
	var tail int
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show recent Floci container logs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			tail = normalizeLogTail(tail)
			logs, err := service.Logs(cmd.Context(), definition, dir, tail)
			if err != nil {
				return convert(err)
			}
			if format == "json" {
				return emit(cmd, "logs", format, map[string]any{"tail": tail, "logs": string(logs)}, nil)
			}
			_, err = fmt.Fprint(cmd.OutOrStdout(), string(logs))
			return err
		},
	}
	project.bind(cmd)
	cmd.Flags().IntVar(&tail, "tail", 200, "maximum log lines to return")
	return cmd
}

func normalizeLogTail(tail int) int {
	if tail <= 0 || tail > 10000 {
		return 200
	}
	return tail
}
