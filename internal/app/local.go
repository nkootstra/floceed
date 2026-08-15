package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
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
	if wait <= 0 {
		wait = time.Duration(p.Target.HookTimeoutSeconds+30) * time.Second
	}
	upCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	target := filepath.Join(projectDir, p.Output.Directory)
	composeFile := filepath.Join(target, "compose.generated.yaml")
	if output, err := a.localRuntime.Start(upCtx, target, composeFile); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if upCtx.Err() == context.DeadlineExceeded {
			return flociReadyTimeoutError()
		}
		return &Error{Kind: ErrorLocal, Code: "COMPOSE_UP_FAILED", Message: fmt.Sprintf("docker compose up failed: %s", output), Err: err}
	}
	remaining := wait
	if deadline, ok := upCtx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	if remaining <= 0 {
		return flociReadyTimeoutError()
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/_floci/init", p.Target.Port)
	err := a.localRuntime.WaitReady(upCtx, url, remaining)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if upCtx.Err() == context.DeadlineExceeded {
		return flociReadyTimeoutError()
	}
	return err
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
