package cli

import (
	"os"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/policy"
	"github.com/spf13/cobra"
)

func fixtureCommand() *cobra.Command {
	root := &cobra.Command{Use: "fixture", Short: "Verify and admit local CI fixtures"}
	var input, output, policyPath string
	verify := &cobra.Command{Use: "verify", Short: "Verify a generated fixture without AWS access", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		format, err := validateOutput(output)
		if err != nil {
			return err
		}
		if strings.TrimSpace(input) == "" {
			return usage("FIXTURE_INPUT_REQUIRED", "--input is required")
		}
		result, err := bundle.VerifyFixture(input)
		if err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "FIXTURE_INVALID", Message: err.Error(), Remediation: "provide a complete generated bundle directory or rerun floceed pull"}
		}
		return emit(cmd, "fixture verify", format, result, nil)
	}}
	verify.Flags().StringVar(&input, "input", "", "generated fixture directory")
	verify.Flags().StringVar(&output, "output", "text", "output format: text or json")
	root.AddCommand(verify)
	admit := &cobra.Command{Use: "admit", Short: "Evaluate a verified fixture against a local admission policy", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		format, err := validateOutput(output)
		if err != nil {
			return err
		}
		if strings.TrimSpace(input) == "" {
			return usage("FIXTURE_INPUT_REQUIRED", "--input is required")
		}
		if strings.TrimSpace(policyPath) == "" {
			return usage("FIXTURE_POLICY_REQUIRED", "--policy is required")
		}
		result, err := bundle.VerifyFixture(input)
		if err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "FIXTURE_INVALID", Message: err.Error()}
		}
		policyBytes, err := os.ReadFile(policyPath)
		if err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "POLICY_INVALID", Message: err.Error()}
		}
		admission, err := policy.Load(policyBytes)
		if err != nil {
			return &CommandError{Kind: KindUsage, Code: "POLICY_INVALID", Message: err.Error()}
		}
		generated, err := bundle.LoadGenerated(cmd.Context(), input)
		if err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "FIXTURE_INVALID", Message: err.Error()}
		}
		decision := admission.Evaluate(policy.Facts{Identity: result.Identity, Manifest: generated.Manifest, CapturedAt: generated.Manifest.Capture.CapturedAt, Provenance: result.Provenance, TrustedProducer: policy.TrustedProducerFromEnvironment()}, time.Now())
		if !decision.Allowed {
			return &CommandError{Kind: KindLocal, Code: "FIXTURE_ADMISSION_REJECTED", Message: "fixture admission rejected", Data: decision}
		}
		return emit(cmd, "fixture admit", format, decision, nil)
	}}
	admit.Flags().StringVar(&input, "input", "", "generated fixture directory")
	admit.Flags().StringVar(&policyPath, "policy", "", "admission policy file")
	admit.Flags().StringVar(&output, "output", "text", "output format: text or json")
	root.AddCommand(admit)
	var archive, target string
	pack := &cobra.Command{Use: "pack", Short: "Pack a verified fixture into a deterministic archive", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(input) == "" || strings.TrimSpace(archive) == "" {
			return usage("FIXTURE_PATH_REQUIRED", "--input and --archive are required")
		}
		if err := bundle.PackFixture(cmd.Context(), input, archive); err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "FIXTURE_PACK_FAILED", Message: err.Error()}
		}
		return emit(cmd, "fixture pack", output, map[string]any{"archive": archive}, nil)
	}}
	pack.Flags().StringVar(&input, "input", "", "verified fixture directory")
	pack.Flags().StringVar(&archive, "archive", "", "output archive path")
	pack.Flags().StringVar(&output, "output", "text", "output format: text or json")
	root.AddCommand(pack)
	unpack := &cobra.Command{Use: "unpack", Short: "Safely unpack a fixture archive", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(archive) == "" || strings.TrimSpace(target) == "" {
			return usage("FIXTURE_PATH_REQUIRED", "--archive and --target are required")
		}
		if err := bundle.UnpackFixture(cmd.Context(), archive, target); err != nil {
			return &CommandError{Kind: KindFilesystem, Code: "FIXTURE_UNPACK_FAILED", Message: err.Error()}
		}
		return emit(cmd, "fixture unpack", output, map[string]any{"target": target}, nil)
	}}
	unpack.Flags().StringVar(&archive, "archive", "", "input archive path")
	unpack.Flags().StringVar(&target, "target", "", "output fixture directory")
	unpack.Flags().StringVar(&output, "output", "text", "output format: text or json")
	root.AddCommand(unpack)
	return root
}
