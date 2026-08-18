package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/storage"
	"github.com/spf13/cobra"
)

func initCommand() *cobra.Command {
	var projectPath, region, profile, expectedAccountID, output string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create a minimal floceed.yaml project",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := validateOutput(output)
			if err != nil {
				return err
			}
			if strings.TrimSpace(region) == "" {
				return usage("REGION_REQUIRED", "--region is required")
			}
			project := config.NewProject()
			project.Source.Region = strings.TrimSpace(region)
			project.Source.Profile = strings.TrimSpace(profile)
			project.Source.ExpectedAccountID = strings.TrimSpace(expectedAccountID)
			data, err := config.Encode(project)
			if err != nil {
				return &CommandError{Kind: KindUsage, Code: "PROJECT_INVALID", Message: err.Error(), Err: err}
			}
			absolute, err := filepath.Abs(projectPath)
			if err != nil {
				return &CommandError{Kind: KindFilesystem, Code: "PROJECT_PATH_INVALID", Message: err.Error(), Err: err}
			}
			if err := writeProjectFile(absolute, data, force); err != nil {
				code, remediation := "PROJECT_WRITE_FAILED", "ensure the project directory exists and is writable"
				if os.IsExist(err) {
					code, remediation = "PROJECT_EXISTS", "choose another --project path or pass --force to replace it"
				}
				return &CommandError{Kind: KindFilesystem, Code: code, Message: fmt.Sprintf("write %s: %v", absolute, err), Remediation: remediation, Err: err}
			}
			return emit(cmd, "init", format, map[string]any{"project": absolute, "overwritten": force}, nil)
		},
	}
	cmd.Flags().StringVar(&projectPath, "project", "floceed.yaml", "project configuration file to create")
	cmd.Flags().StringVar(&region, "region", "", "AWS region")
	cmd.Flags().StringVar(&profile, "profile", "", "AWS profile")
	cmd.Flags().StringVar(&expectedAccountID, "expected-account-id", "", "expected 12-digit AWS account ID")
	cmd.Flags().BoolVar(&force, "force", false, "replace an existing project file")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func writeProjectFile(path string, data []byte, force bool) error {
	if force {
		return storage.WriteFileSync(path, data)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".floceed-init-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		return err
	}
	return storage.SyncDir(dir)
}
