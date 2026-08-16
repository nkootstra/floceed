package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type Service interface {
	Scan(context.Context, app.ScanRequest) (app.ScanResult, error)
	PlanWithOptions(context.Context, config.Project, app.PlanOptions) (app.Plan, error)
	PullWithOptions(context.Context, config.Project, string, string, string, app.PullOptions) (app.PullResult, error)
	Render(context.Context, config.Project, string) (model.Manifest, error)
	Doctor(context.Context, config.Project, string, string, string) (app.DoctorResult, error)
	Up(context.Context, config.Project, string, time.Duration) error
}

type Options struct {
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Version string
	App     Service
	TUI     func(context.Context, io.Reader, io.Writer, bool, string) error
}

func New(options Options) *cobra.Command {
	if options.Stdin == nil {
		options.Stdin = os.Stdin
	}
	if options.Stdout == nil {
		options.Stdout = os.Stdout
	}
	if options.Stderr == nil {
		options.Stderr = os.Stderr
	}
	if options.App == nil {
		options.App = app.New(options.Version)
	}
	var noColor bool
	var fixtureProfile string
	root := &cobra.Command{Use: "floceed", Short: "Compile AWS resource snapshots into portable Floci bundles", SilenceErrors: true, SilenceUsage: true}
	root.SetIn(options.Stdin)
	root.SetOut(options.Stdout)
	root.SetErr(options.Stderr)
	root.PersistentFlags().BoolVar(&noColor, "no-color", false, "disable colored output")
	root.Flags().StringVar(&fixtureProfile, "fixture-profile", "", "select fixture governance profile in interactive mode")
	root.AddCommand(scanCommand(options.App), planCommand(options.App), pullCommand(options.App), renderCommand(options.App), doctorCommand(options.App), upCommand(options.App))
	root.AddCommand(&cobra.Command{Use: "version", Short: "Print version information", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		version := options.Version
		if version == "" {
			version = "dev"
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), version)
		return err
	}})
	root.RunE = func(cmd *cobra.Command, args []string) error {
		if isTerminal(options.Stdin) && isTerminal(options.Stdout) && options.TUI != nil {
			return options.TUI(cmd.Context(), options.Stdin, options.Stdout, noColor || os.Getenv("NO_COLOR") != "", fixtureProfile)
		}
		return cmd.Help()
	}
	return root
}

func scanCommand(service Service) *cobra.Command {
	var profile, region, output string
	cmd := &cobra.Command{Use: "scan", Short: "Discover supported AWS resources", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if region == "" {
			return usage("REGION_REQUIRED", "--region is required")
		}
		format, err := validateOutput(output)
		if err != nil {
			return err
		}
		result, err := service.Scan(cmd.Context(), app.ScanRequest{Profile: profile, Region: region})
		if err != nil {
			return convert(err)
		}
		return emit(cmd, "scan", format, result, result.Findings)
	}}
	cmd.Flags().StringVar(&profile, "profile", "", "AWS profile")
	cmd.Flags().StringVar(&region, "region", "", "AWS region")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

type projectOptions struct {
	filename string
	output   string
}

func (options *projectOptions) bind(cmd *cobra.Command) {
	cmd.Flags().StringVar(&options.filename, "project", app.DefaultProjectFile, "project configuration file")
	cmd.Flags().StringVar(&options.output, "output", "text", "output format: text or json")
}

func (options projectOptions) load() (config.Project, string, string, error) {
	format, err := validateOutput(options.output)
	if err != nil {
		return config.Project{}, "", "", err
	}
	project, dir, err := loadProject(options.filename)
	if err != nil {
		return config.Project{}, "", "", usage("PROJECT_INVALID", err.Error())
	}
	return project, dir, format, nil
}

type sourceOverrides struct {
	profile        string
	region         string
	fixtureProfile string
}

func (overrides *sourceOverrides) bind(cmd *cobra.Command) {
	cmd.Flags().StringVar(&overrides.profile, "profile", "", "override AWS profile")
	cmd.Flags().StringVar(&overrides.region, "region", "", "override AWS region")
}

func (overrides *sourceOverrides) bindFixtureProfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&overrides.fixtureProfile, "fixture-profile", "", "select fixture governance profile")
}

func planCommand(service Service) *cobra.Command {
	var project projectOptions
	var source sourceOverrides
	cmd := &cobra.Command{Use: "plan", Short: "Preview capture and replay actions", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		definition, _, format, err := project.load()
		if err != nil {
			return err
		}
		result, err := service.PlanWithOptions(cmd.Context(), definition, app.PlanOptions{AWSProfile: source.profile, Region: source.region, FixtureProfile: source.fixtureProfile})
		if err != nil {
			return convert(err)
		}
		return emit(cmd, "plan", format, result, result.Findings)
	}}
	project.bind(cmd)
	source.bind(cmd)
	source.bindFixtureProfile(cmd)
	return cmd
}

func pullCommand(service Service) *cobra.Command {
	var project projectOptions
	var source sourceOverrides
	var yes bool
	var progress, workDir string
	var restart bool
	cmd := &cobra.Command{Use: "pull", Short: "Capture selected AWS resources and generate a bundle", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		definition, dir, format, err := project.load()
		if err != nil {
			return err
		}
		if !yes && !isTerminal(cmd.InOrStdin()) {
			return usage("CONFIRMATION_REQUIRED", "pull in a non-interactive terminal requires --yes")
		}
		progressMode, err := validateProgress(progress)
		if err != nil {
			return err
		}
		if progressMode == "auto" {
			if isTerminal(cmd.ErrOrStderr()) {
				progressMode = "plain"
			} else {
				progressMode = "off"
			}
		}
		report := progressReporter(cmd.ErrOrStderr(), progressMode)
		result, err := service.PullWithOptions(cmd.Context(), definition, dir, source.profile, source.region, app.PullOptions{WorkDir: workDir, Restart: restart, Progress: report, FixtureProfile: source.fixtureProfile})
		if err != nil {
			return convert(err)
		}
		return emit(cmd, "pull", format, result, result.Manifest.Findings)
	}}
	project.bind(cmd)
	source.bind(cmd)
	source.bindFixtureProfile(cmd)
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm capture and bundle replacement")
	cmd.Flags().StringVar(&progress, "progress", "auto", "progress output: auto, plain, json, or off")
	cmd.Flags().StringVar(&workDir, "work-dir", "", "capture checkpoint directory (defaults to the user cache)")
	cmd.Flags().BoolVar(&restart, "restart", false, "discard the matching capture checkpoint and start again")
	return cmd
}

func validateProgress(value string) (string, error) {
	switch value {
	case "auto", "plain", "json", "off":
		return value, nil
	default:
		return "", usage("PROGRESS_INVALID", "--progress must be auto, plain, json, or off")
	}
}
func progressReporter(w io.Writer, mode string) func(model.ProgressEvent) {
	if mode == "off" {
		return nil
	}
	return func(event model.ProgressEvent) {
		if mode == "json" {
			_ = json.NewEncoder(w).Encode(event)
			return
		}
		parts := make([]string, 0, 4)
		if event.Phase != "" {
			parts = append(parts, event.Phase)
		}
		if event.Service != "" {
			parts = append(parts, event.Service)
		}
		if event.Resource != "" {
			parts = append(parts, event.Resource)
		}
		if event.TotalRecords > 0 {
			marker := "~"
			if event.TotalPrecision == "exact" {
				marker = ""
			}
			parts = append(parts, fmt.Sprintf("%d/%s%d", event.CompletedRecords, marker, event.TotalRecords))
		}
		if len(parts) > 0 {
			_, _ = fmt.Fprintf(w, "%s\n", strings.Join(parts, " "))
		}
	}
}

func renderCommand(service Service) *cobra.Command {
	var project projectOptions
	cmd := &cobra.Command{Use: "render", Short: "Regenerate files from a local snapshot", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		definition, dir, format, err := project.load()
		if err != nil {
			return err
		}
		result, err := service.Render(cmd.Context(), definition, dir)
		if err != nil {
			return convert(err)
		}
		return emit(cmd, "render", format, result, result.Findings)
	}}
	project.bind(cmd)
	return cmd
}

func doctorCommand(service Service) *cobra.Command {
	var project projectOptions
	var source sourceOverrides
	cmd := &cobra.Command{Use: "doctor", Short: "Check AWS, Docker, and project prerequisites", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		definition, dir, format, err := project.load()
		if err != nil {
			return err
		}
		result, err := service.Doctor(cmd.Context(), definition, dir, source.profile, source.region)
		if err != nil {
			converted := convert(err)
			if format == "text" {
				// Doctor always returns partial checks with a failure; print
				// them before returning so the summary lands on stderr while
				// the machine-readable details stay on stdout.
				if len(result.Checks) > 0 {
					if emitErr := emit(cmd, "doctor", format, result, nil); emitErr != nil {
						return fmt.Errorf("%w (write failed: %v)", converted, emitErr)
					}
				}
				return converted
			}
			return withData(converted, result)
		}
		return emit(cmd, "doctor", format, result, nil)
	}}
	project.bind(cmd)
	source.bind(cmd)
	return cmd
}

func upCommand(service Service) *cobra.Command {
	var project projectOptions
	var wait time.Duration
	var progress string
	cmd := &cobra.Command{Use: "up", Short: "Start the generated Floci Compose project", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		definition, dir, format, err := project.load()
		if err != nil {
			return err
		}
		progressMode, err := validateProgress(progress)
		if err != nil {
			return err
		}
		if progressMode == "auto" {
			if isTerminal(cmd.ErrOrStderr()) {
				progressMode = "plain"
			} else {
				progressMode = "off"
			}
		}
		report := progressReporter(cmd.ErrOrStderr(), progressMode)
		if advanced, ok := service.(interface {
			UpWithOptions(context.Context, config.Project, string, app.UpOptions) error
		}); ok {
			err = advanced.UpWithOptions(cmd.Context(), definition, dir, app.UpOptions{Wait: wait, Progress: report})
		} else {
			err = service.Up(cmd.Context(), definition, dir, wait)
		}
		if err != nil {
			return convert(err)
		}
		return emit(cmd, "up", format, map[string]any{"ready": true, "port": definition.Target.Port}, nil)
	}}
	project.bind(cmd)
	cmd.Flags().DurationVar(&wait, "wait", 0, "readiness timeout")
	cmd.Flags().StringVar(&progress, "progress", "auto", "progress output: auto, plain, json, or off")
	return cmd
}

func loadProject(filename string) (config.Project, string, error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return config.Project{}, "", err
	}
	f, err := os.Open(absolute)
	if err != nil {
		return config.Project{}, "", err
	}
	defer f.Close()
	p, err := config.Decode(f)
	return p, filepath.Dir(absolute), err
}
func validateOutput(value string) (string, error) {
	if value != "text" && value != "json" {
		return "", usage("OUTPUT_INVALID", "--output must be text or json")
	}
	return value, nil
}
func emit(cmd *cobra.Command, name, format string, data any, findings any) error {
	if format == "json" {
		status := StatusSuccess
		if countFindings(findings) > 0 {
			status = StatusSuccessWithFindings
		}
		return WriteJSON(cmd.OutOrStdout(), Envelope{Command: name, Status: status, Data: data, Findings: findings})
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), string(b))
	return err
}
func countFindings(v any) int {
	switch x := v.(type) {
	case []model.Finding:
		return len(x)
	default:
		return 0
	}
}
func usage(code, message string) error {
	return &CommandError{Kind: KindUsage, Code: code, Message: message}
}
func convert(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	var e *app.Error
	if errors.As(err, &e) {
		kind := KindUnexpected
		switch e.Kind {
		case app.ErrorUsage:
			kind = KindUsage
		case app.ErrorSource:
			kind = KindSource
		case app.ErrorPartial:
			kind = KindPartial
		case app.ErrorPlan:
			kind = KindPlan
		case app.ErrorFilesystem:
			kind = KindFilesystem
		case app.ErrorLocal:
			kind = KindLocal
		}
		return &CommandError{Kind: kind, Code: e.Code, Message: e.Message, Remediation: e.Remediation, Err: err}
	}
	return err
}

// withData attaches a payload to a *CommandError so WriteInvocationError
// emits structured data alongside the error detail. Only doctor uses it, to
// deliver its partial check results in the JSON error envelope; it passes
// errors through unchanged when they are not CommandErrors (for example,
// context cancellation).
func withData(err error, data any) error {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return err
	}
	copy := *commandErr
	copy.Data = data
	return &copy
}

type fdHolder interface{ Fd() uintptr }

func isTerminal(v any) bool {
	switch value := v.(type) {
	case fdHolder:
		return term.IsTerminal(int(value.Fd()))
	default:
		return false
	}
}
