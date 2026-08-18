package cli

import (
	"github.com/nkootstra/floceed/internal/app"
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
				return emit(cmd, "status", format, result, result.Findings)
			}
			return writeInspectionText(cmd.OutOrStdout(), result)
		},
	}
	project.bind(cmd)
	return cmd
}
