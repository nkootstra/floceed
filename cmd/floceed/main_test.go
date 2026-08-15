package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/cli"
	"github.com/spf13/cobra"
)

func TestResolveVersion(t *testing.T) {
	tests := []struct {
		name     string
		injected string
		info     *debug.BuildInfo
		ok       bool
		want     string
	}{
		{name: "linker override", injected: "v9.9.9", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.1.1"}}, ok: true, want: "v9.9.9"},
		{name: "module version", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.1.1"}}, ok: true, want: "v0.1.1"},
		{name: "local source build", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.1.0"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}}}, ok: true, want: "dev"},
		{name: "dirty source build", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.1.0+dirty"}, Settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}}}, ok: true, want: "dev"},
		{name: "development build", info: &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, ok: true, want: "dev"},
		{name: "empty module version", info: &debug.BuildInfo{}, ok: true, want: "dev"},
		{name: "missing build info", want: "dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveVersion(tt.injected, tt.info, tt.ok); got != tt.want {
				t.Fatalf("resolveVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecuteWritesJSONEnvelopeForJSONInvocationError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cmd := cli.New(cli.Options{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"scan", "--output", "json"})

	if code := execute(context.Background(), cmd, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	var envelope cli.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != 1 || envelope.Command != "scan" || envelope.Status != cli.StatusError {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if envelope.Error == nil || envelope.Error.Code != "REGION_REQUIRED" {
		t.Fatalf("unexpected error detail: %+v", envelope.Error)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

func TestExecutePreservesTextInvocationError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cmd := cli.New(cli.Options{Stdout: &stdout, Stderr: &stderr})
	cmd.SetArgs([]string{"scan"})

	if code := execute(context.Background(), cmd, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "--region is required") {
		t.Fatalf("unexpected stderr: %s", got)
	}
}

func TestExecuteKeepsDoctorChecksOnStdoutAndSummaryOnStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cmd := &cobra.Command{
		Use: "doctor",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), `{"checks":[{"name":"aws","ok":false}]}`)
			return &cli.CommandError{Kind: cli.KindLocal, Code: "DOCTOR_FAILED", Message: "one or more prerequisite checks failed"}
		},
	}
	cmd.SetOut(&stdout)

	if code := execute(context.Background(), cmd, &stderr); code != 7 {
		t.Fatalf("exit code = %d, want 7", code)
	}
	if got := stdout.String(); !strings.Contains(got, `"name":"aws"`) {
		t.Fatalf("stdout omitted checks: %s", got)
	}
	if got := stderr.String(); got != "one or more prerequisite checks failed\n" {
		t.Fatalf("stderr = %q", got)
	}
}
