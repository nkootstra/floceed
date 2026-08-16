package config

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodeRejectsUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader("schema_version: 1\nunknown: true\n"))
	if err == nil {
		t.Fatal("expected strict decoding error")
	}
}

func TestValidateDataRequiresBounds(t *testing.T) {
	c := Project{SchemaVersion: 1, Source: Source{Region: "eu-west-1"}, Resources: Resources{S3: []S3Resource{{Name: "bucket", Data: &S3DataPolicy{Enabled: true}}}}}
	if err := c.Validate(); err == nil {
		t.Fatal("expected enabled data without limits to fail")
	}
}

func TestValidateRejectsParentOutputDirectory(t *testing.T) {
	c := Project{SchemaVersion: 1, Source: Source{Region: "eu-west-1"}, Output: Output{Directory: ".."}}
	if err := c.Validate(); err == nil {
		t.Fatal("expected parent output directory to fail")
	}
}

func TestValidateRejectsOutputDirectoryResolvingToProjectRoot(t *testing.T) {
	for _, directory := range []string{".", "./", "subdir/.."} {
		c := Project{SchemaVersion: 1, Source: Source{Region: "eu-west-1"}, Output: Output{Directory: directory}}
		if err := c.Validate(); err == nil {
			t.Errorf("expected output directory %q to fail", directory)
		}
	}
}

func TestValidateRejectsDuplicateResources(t *testing.T) {
	tests := []struct {
		name    string
		project Project
		want    string
	}{
		{
			name: "S3 bucket",
			project: Project{
				SchemaVersion: CurrentSchemaVersion,
				Source:        Source{Region: "eu-west-1"},
				Resources:     Resources{S3: []S3Resource{{Name: "assets"}, {Name: "assets"}}},
			},
			want: `duplicate S3 resource "assets"`,
		},
		{
			name: "DynamoDB table",
			project: Project{
				SchemaVersion: CurrentSchemaVersion,
				Source:        Source{Region: "eu-west-1"},
				Resources:     Resources{DynamoDB: []DynamoDBResource{{Name: "orders"}, {Name: "orders"}}},
			},
			want: `duplicate DynamoDB resource "orders"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.project.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.want)
			}
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() error = %v, want errors.Is(ErrValidation)", err)
			}
		})
	}
}

func TestDecodeAppliesProjectAndCaptureDefaults(t *testing.T) {
	project, err := Decode(strings.NewReader(`
schema_version: 1
source:
  region: eu-west-1
resources:
  s3:
    - name: assets
      data:
        enabled: true
        max_objects: 1
        max_object_bytes: 2
        max_total_bytes: 3
`))
	if err != nil {
		t.Fatal(err)
	}
	if project.Target.FlociVersion != DefaultFlociVersion || project.Target.Port != DefaultPort || project.Target.HookTimeoutSeconds != DefaultHookTimeoutSeconds {
		t.Fatalf("target defaults = %#v", project.Target)
	}
	if project.Output.Directory != ".floceed" {
		t.Fatalf("output directory = %q", project.Output.Directory)
	}
	if got := project.Resources.S3[0].Data.Overwrite; got != OverwriteIfDifferent {
		t.Fatalf("overwrite = %q, want %q", got, OverwriteIfDifferent)
	}
}

func TestDecodePreservesExplicitOverwritePolicy(t *testing.T) {
	project, err := Decode(strings.NewReader(`
schema_version: 1
source:
  region: eu-west-1
resources:
  s3:
    - name: assets
      data:
        enabled: true
        max_objects: 1
        max_object_bytes: 2
        max_total_bytes: 3
        overwrite: never
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := project.Resources.S3[0].Data.Overwrite; got != OverwriteNever {
		t.Fatalf("overwrite = %q, want %q", got, OverwriteNever)
	}
}

func TestCapturePolicyDefaultsValidate(t *testing.T) {
	project := NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.S3 = []S3Resource{{Name: "assets", Data: NewS3DataPolicy()}}
	project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders", Data: NewDynamoDBDataPolicy()}}
	if err := project.Validate(); err != nil {
		t.Fatalf("default capture policies do not validate: %v", err)
	}
}

func TestFullDataModeRequiresExplicitReplayTimeoutAndNoBoundedLimits(t *testing.T) {
	project := NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders", Data: &DynamoDBDataPolicy{Enabled: true, Mode: DataModeFull}}}
	if err := project.Validate(); err == nil || !strings.Contains(err.Error(), "hook_timeout_seconds") {
		t.Fatalf("validation error = %v", err)
	}
	project.Target.HookTimeoutSeconds = 3600
	if err := project.Validate(); err != nil {
		t.Fatalf("full project should validate: %v", err)
	}
	project.Resources.DynamoDB[0].Data.MaxItems = 1
	if err := project.Validate(); err == nil || !strings.Contains(err.Error(), "cannot set bounded limits") {
		t.Fatalf("limit error = %v", err)
	}
}
