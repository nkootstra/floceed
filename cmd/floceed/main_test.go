package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/cli"
)

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
