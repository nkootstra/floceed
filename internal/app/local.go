package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
)

const defaultDockerProbeTimeout = 10 * time.Second
const defaultReadyPollInterval = 500 * time.Millisecond

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type localRuntime interface {
	DoctorChecks(context.Context) []Check
	Start(context.Context, string, string) ([]byte, error)
	WaitReady(context.Context, string, time.Duration) error
	InspectStatus(context.Context, string, time.Duration) (inspection.Runtime, error)
}
type progressRuntime interface {
	WatchProgress(context.Context, string, string, time.Time, func(model.ProgressEvent)) func()
}
type UpOptions struct {
	Wait     time.Duration
	Progress func(model.ProgressEvent)
}

type dockerLocalRuntime struct {
	lookPath      func(string) (string, error)
	dockerCommand func(context.Context, ...string) ([]byte, error)
	httpClient    httpDoer
	probeTimeout  time.Duration
	pollInterval  time.Duration
}

func newDockerLocalRuntime() *dockerLocalRuntime {
	return &dockerLocalRuntime{
		lookPath:      exec.LookPath,
		dockerCommand: runDockerCommand,
		httpClient:    &http.Client{Timeout: 2 * time.Second},
		probeTimeout:  defaultDockerProbeTimeout,
		pollInterval:  defaultReadyPollInterval,
	}
}

type Check struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}
type DoctorResult struct {
	Checks []Check `json:"checks"`
}

func (a *Application) Doctor(ctx context.Context, p config.Project, projectDir, profile, region string) (DoctorResult, error) {
	result := DoctorResult{}
	if err := p.Validate(); err != nil {
		result.Checks = append(result.Checks, Check{"project", false, err.Error()})
	} else {
		result.Checks = append(result.Checks, Check{"project", true, "configuration is valid"})
	}
	if profile == "" {
		profile = p.Source.Profile
	}
	if region == "" {
		region = p.Source.Region
	}
	if _, err := a.Factory.Open(ctx, SourceRequest{
		Profile:       profile,
		Region:        region,
		S3Names:       s3Names(p),
		DynamoDBNames: ddbNames(p),
	}); err != nil {
		result.Checks = append(result.Checks, Check{"aws", false, err.Error()})
	} else {
		result.Checks = append(result.Checks, Check{"aws", true, "caller identity confirmed"})
	}
	result.Checks = append(result.Checks, a.localRuntime.DoctorChecks(ctx)...)
	parent := filepath.Dir(filepath.Join(projectDir, p.Output.Directory))
	if err := os.MkdirAll(parent, 0700); err != nil {
		result.Checks = append(result.Checks, Check{"output", false, err.Error()})
	} else if f, err := os.CreateTemp(parent, ".floceed-write-check-"); err != nil {
		result.Checks = append(result.Checks, Check{"output", false, err.Error()})
	} else {
		name := f.Name()
		f.Close()
		os.Remove(name)
		result.Checks = append(result.Checks, Check{"output", true, "output directory is writable"})
	}
	for _, c := range result.Checks {
		if !c.OK {
			return result, &Error{Kind: ErrorLocal, Code: "DOCTOR_FAILED", Message: "one or more prerequisite checks failed"}
		}
	}
	return result, nil
}

func runDockerCommand(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

func runComposeUp(ctx context.Context, target, composeFile string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "up", "-d")
	cmd.Dir = target
	return cmd.CombinedOutput()
}

func (r *dockerLocalRuntime) runDockerProbe(ctx context.Context, args ...string) ([]byte, error) {
	probeCtx, cancel := context.WithTimeout(ctx, r.probeTimeout)
	defer cancel()
	return r.dockerCommand(probeCtx, args...)
}

func (r *dockerLocalRuntime) DoctorChecks(ctx context.Context) []Check {
	if _, err := r.lookPath("docker"); err != nil {
		return []Check{{"docker", false, "docker executable not found"}}
	}
	if output, err := r.runDockerProbe(ctx, "compose", "version"); err != nil {
		return []Check{{"docker", false, string(output)}}
	}
	if output, err := r.runDockerProbe(ctx, "info"); err != nil {
		return []Check{{"docker", false, string(output)}}
	}
	checks := []Check{{"docker", true, "Docker and Compose are available"}}
	if output, err := r.runDockerProbe(ctx, "manifest", "inspect", compose.Image); err != nil {
		return append(checks, Check{"floci-image", false, fmt.Sprintf("pinned Floci image is unavailable: %s", output)})
	}
	return append(checks, Check{"floci-image", true, "pinned Floci 1.6.0 compat image is available"})
}

func (r *dockerLocalRuntime) Start(ctx context.Context, target, composeFile string) ([]byte, error) {
	return runComposeUp(ctx, target, composeFile)
}

type initStatus struct {
	Completed struct {
		Ready bool `json:"ready"`
	} `json:"completed"`
	Scripts struct {
		Ready []struct {
			Script     string `json:"script"`
			State      string `json:"state"`
			ReturnCode int    `json:"return_code"`
		} `json:"ready"`
	} `json:"scripts"`
}

func (a *Application) Up(ctx context.Context, p config.Project, projectDir string, wait time.Duration) error {
	return a.UpWithOptions(ctx, p, projectDir, UpOptions{Wait: wait})
}

func (a *Application) UpWithOptions(ctx context.Context, p config.Project, projectDir string, options UpOptions) error {
	wait := options.Wait
	if err := ctx.Err(); err != nil {
		return err
	}
	target := filepath.Join(projectDir, p.Output.Directory)
	composeFile := filepath.Join(target, bundle.ComposeFile)
	// The bundle is installed atomically by render/pull, so a regular
	// Compose entry implies the rest of the bundle exists. Gate on it
	// before invoking Docker to fail fast with actionable remediation.
	// These structural errors intentionally carry no wrapped cause; the
	// message includes the offending path. Lstat (not Stat) so a symlink
	// cannot masquerade as a rendered entry.
	info, err := os.Lstat(composeFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Error{
				Kind:        ErrorFilesystem,
				Code:        "BUNDLE_MISSING",
				Message:     fmt.Sprintf("generated Compose file not found: %s", composeFile),
				Remediation: "Run floceed render if a valid local manifest exists; otherwise run floceed pull for this project.",
			}
		}
		return filesystemError(err)
	}
	if !info.Mode().IsRegular() {
		return &Error{
			Kind:        ErrorFilesystem,
			Code:        "BUNDLE_INVALID",
			Message:     fmt.Sprintf("generated Compose path is not a regular file: %s", composeFile),
			Remediation: "Run floceed render if a valid local manifest exists; otherwise run floceed pull for this project.",
		}
	}
	if wait <= 0 {
		wait = time.Duration(p.Target.HookTimeoutSeconds+30) * time.Second
	}
	upCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	startedAt := time.Now()
	if options.Progress != nil {
		options.Progress(model.ProgressEvent{SchemaVersion: 1, Event: "progress", Operation: "replay", Phase: "start", Message: "starting Floci"})
	}
	if output, err := a.localRuntime.Start(upCtx, target, composeFile); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if upCtx.Err() == context.DeadlineExceeded {
			return flociReadyTimeoutError()
		}
		return &Error{Kind: ErrorLocal, Code: "COMPOSE_UP_FAILED", Message: fmt.Sprintf("docker compose up failed: %s", output), Err: err}
	}
	stopProgress := func() {}
	if runtime, ok := a.localRuntime.(progressRuntime); ok && options.Progress != nil {
		stopProgress = runtime.WatchProgress(upCtx, target, composeFile, startedAt, options.Progress)
	}
	defer stopProgress()
	remaining := wait
	if deadline, ok := upCtx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	if remaining <= 0 {
		return flociReadyTimeoutError()
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/_floci/init", p.Target.Port)
	err = a.localRuntime.WaitReady(upCtx, url, remaining)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if upCtx.Err() == context.DeadlineExceeded {
		return flociReadyTimeoutError()
	}
	if err == nil && options.Progress != nil {
		options.Progress(model.ProgressEvent{SchemaVersion: 1, Event: "progress", Operation: "replay", Phase: "complete", Message: "Floci replay completed"})
	}
	return err
}

func (r *dockerLocalRuntime) WatchProgress(ctx context.Context, target, composeFile string, since time.Time, report func(model.ProgressEvent)) func() {
	watchCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(watchCtx, "docker", "compose", "-f", composeFile, "logs", "--follow", "--no-color", "--no-log-prefix", "--since", since.UTC().Format(time.RFC3339Nano), "floci")
	cmd.Dir = target
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return cancel
	}
	if err = cmd.Start(); err != nil {
		return cancel
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if event, ok := decodeProgressLine(line); ok {
				report(event)
			}
		}
		_ = cmd.Wait()
	}()
	return cancel
}

func decodeProgressLine(line string) (model.ProgressEvent, bool) {
	const prefix = "FLOCEED_PROGRESS "
	index := strings.Index(line, prefix)
	if index < 0 {
		return model.ProgressEvent{}, false
	}
	var event model.ProgressEvent
	if json.Unmarshal([]byte(line[index+len(prefix):]), &event) != nil || event.Event != "progress" {
		return model.ProgressEvent{}, false
	}
	return event, true
}

func flociReadyTimeoutError() error {
	return &Error{Kind: ErrorLocal, Code: "FLOCI_READY_TIMEOUT", Message: "Floci initialization did not complete before the timeout"}
}

func (r *dockerLocalRuntime) WaitReady(ctx context.Context, url string, wait time.Duration) error {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return flociReadyTimeoutError()
		case <-ticker.C:
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			response, err := r.httpClient.Do(req)
			if err != nil {
				continue
			}
			var status initStatus
			decodeErr := json.NewDecoder(response.Body).Decode(&status)
			response.Body.Close()
			if decodeErr != nil || response.StatusCode/100 != 2 {
				continue
			}
			if status.Completed.Ready {
				return nil
			}
			for _, script := range status.Scripts.Ready {
				if strings.EqualFold(script.State, "ERROR") || strings.EqualFold(script.State, "FAILED") || script.ReturnCode != 0 {
					return &Error{Kind: ErrorLocal, Code: "FLOCI_INIT_FAILED", Message: fmt.Sprintf("Floci ready hook %s failed", script.Script)}
				}
			}
		}
	}
}

// InspectStatus performs one bounded, read-only readiness query. Runtime
// failures are data, not artifact failures, so callers can retain a valid
// offline inspection result.
func (r *dockerLocalRuntime) InspectStatus(ctx context.Context, url string, wait time.Duration) (inspection.Runtime, error) {
	if err := ctx.Err(); err != nil {
		return inspection.Runtime{}, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
	if err != nil {
		return unavailableRuntime(err), nil
	}
	response, err := r.httpClient.Do(req)
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return inspection.Runtime{}, parentErr
		}
		return unavailableRuntime(err), nil
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return unavailableRuntime(fmt.Errorf("runtime returned HTTP %d", response.StatusCode)), nil
	}
	var status initStatus
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&status); err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return inspection.Runtime{}, parentErr
		}
		return unavailableRuntime(fmt.Errorf("invalid runtime response")), nil
	}
	failed := make([]string, 0)
	for _, script := range status.Scripts.Ready {
		if strings.EqualFold(script.State, "ERROR") || strings.EqualFold(script.State, "FAILED") || script.ReturnCode != 0 {
			failed = append(failed, boundedMessage(script.Script, 80))
		}
	}
	sort.Strings(failed)
	if status.Completed.Ready && len(failed) == 0 {
		return inspection.Runtime{State: inspection.RuntimeReady}, nil
	}
	return inspection.Runtime{State: inspection.RuntimeNotReady, FailedScripts: failed}, nil
}

func unavailableRuntime(err error) inspection.Runtime {
	return inspection.Runtime{State: inspection.RuntimeUnavailable, Diagnostic: boundedMessage(err.Error(), 160)}
}

func boundedMessage(value string, limit int) string {
	message := strings.Join(strings.Fields(value), " ")
	if len(message) > limit {
		message = message[:limit]
	}
	return message
}
