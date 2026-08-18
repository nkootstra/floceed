package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/nkootstra/floceed/internal/app"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/spf13/cobra"
)

func statusCommand(service Service) *cobra.Command {
	var project projectOptions
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show generated bundle and Floci runtime status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			result, err := service.InspectWithOptions(cmd.Context(), definition, dir, app.InspectOptions{Runtime: true})
			if err != nil {
				return convert(err)
			}
			if format == "json" {
				return emit(cmd, "status", format, result.Runtime, nil)
			}
			return writeStatusText(cmd.OutOrStdout(), result.Runtime)
		},
	}
	project.bind(cmd)
	return cmd
}

func writeStatusText(w io.Writer, status inspection.Runtime) error {
	if _, err := fmt.Fprintf(w, "Runtime: %s\n", strings.ReplaceAll(string(status.State), "_", " ")); err != nil {
		return err
	}
	if status.Diagnostic != "" {
		_, err := fmt.Fprintf(w, "Diagnostic: %s\n", terminalSafe(status.Diagnostic))
		return err
	}
	if len(status.FailedScripts) > 0 {
		_, err := fmt.Fprintf(w, "Failed scripts: %s\n", joinSafe(status.FailedScripts))
		return err
	}
	return nil
}
