package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/governance"
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

func TestValidateKinesisStreamARN(t *testing.T) {
	project := Project{SchemaVersion: CurrentSchemaVersion, Source: Source{Region: "eu-west-1"}, Resources: Resources{Kinesis: []KinesisResource{{Name: "events", ARN: "arn:aws:kinesis:eu-west-1:123456789012:stream/events"}}}}
	if err := project.Validate(); err != nil {
		t.Fatalf("valid Kinesis stream rejected: %v", err)
	}
	project.Resources.Kinesis[0].ARN = "arn:aws:kinesis:eu-west-1:123456789012:stream/other"
	if err := project.Validate(); err == nil {
		t.Fatal("mismatched Kinesis stream ARN accepted")
	}
}

func TestSQSDataIsBoundedOnly(t *testing.T) {
	project := Project{SchemaVersion: CurrentSchemaVersion, Source: Source{Region: "eu-west-1"}, Resources: Resources{SQS: []SQSResource{{Name: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs", Data: &SQSDataPolicy{Enabled: true, Mode: DataModeFull}}}}}
	if err := project.Validate(); err == nil {
		t.Fatal("full SQS message capture should not be accepted")
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

func TestDecodeAcceptsExplicitEventDependencyResources(t *testing.T) {
	project, err := Decode(strings.NewReader(`schema_version: 1
source:
  region: eu-west-1
resources:
  sqs:
    - name: jobs.fifo
      arn: arn:aws:sqs:eu-west-1:123456789012:jobs.fifo
  sns:
    - name: events
      arn: arn:aws:sns:eu-west-1:123456789012:events
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Resources.SQS) != 1 || len(project.Resources.SNS) != 1 {
		t.Fatalf("resources = %#v", project.Resources)
	}
}

func TestDecodeRejectsMismatchedEventDependencyARN(t *testing.T) {
	_, err := Decode(strings.NewReader(`schema_version: 1
source:
  region: eu-west-1
resources:
  sqs:
    - name: jobs
      arn: arn:aws:sqs:eu-west-1:123456789012:other
`))
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateEventDependencyNamesAndPartitions(t *testing.T) {
	validSNS := strings.Repeat("topic", 17)
	validSNS = validSNS[:85]
	tests := []struct {
		name    string
		project Project
		valid   bool
	}{
		{"SQS FIFO", Project{SchemaVersion: CurrentSchemaVersion, Source: Source{Region: "us-gov-west-1"}, Resources: Resources{SQS: []SQSResource{{Name: "jobs.fifo", ARN: "arn:aws-us-gov:sqs:us-gov-west-1:123456789012:jobs.fifo"}}}}, true},
		{"SQS arbitrary dot", Project{SchemaVersion: CurrentSchemaVersion, Source: Source{Region: "eu-west-1"}, Resources: Resources{SQS: []SQSResource{{Name: "jobs.topic", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs.topic"}}}}, false},
		{"SNS long name", Project{SchemaVersion: CurrentSchemaVersion, Source: Source{Region: "eu-west-1"}, Resources: Resources{SNS: []SNSResource{{Name: validSNS, ARN: "arn:aws:sns:eu-west-1:123456789012:" + validSNS}}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.project.Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate() error = %v, valid = %v", err, tt.valid)
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

func TestResolveFixtureProfilePreservesLegacyProject(t *testing.T) {
	project, err := Decode(strings.NewReader("schema_version: 1\nsource:\n  region: eu-west-1\n"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := project.ResolveFixtureProfile("", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if policy != nil {
		t.Fatalf("legacy policy = %#v, want nil", policy)
	}
}

func TestResolveFixtureProfileRequiresSelectionWhenProfilesExist(t *testing.T) {
	project := NewProject()
	project.FixtureProfiles = map[string]FixtureProfile{"safe": {}}
	if _, err := project.ResolveFixtureProfile("", nil); err == nil || !strings.Contains(err.Error(), "fixture profile must be selected") {
		t.Fatalf("selection error = %v", err)
	}
}

func TestFixtureProfileAlgorithmsAreValidatedAndNormalized(t *testing.T) {
	project := NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders", Data: &DynamoDBDataPolicy{Enabled: true, Mode: DataModeFull}}}
	project.Target.HookTimeoutSeconds = 3600
	base := GovernanceRule{ID: "rule-001", Service: governance.ServiceDynamoDB, Resource: "orders", Target: GovernanceTarget{Kind: governance.TargetDynamoDBAttribute, Path: "email"}}
	tests := []struct {
		name      string
		action    governance.Action
		algorithm string
		wantErr   bool
		want      string
	}{
		{"hash defaults", governance.ActionHash, "", false, governance.HashAlgorithm},
		{"hash rejects other", governance.ActionHash, governance.PseudonymAlgorithm, true, ""},
		{"pseudonym defaults", governance.ActionPseudonymize, "", false, governance.PseudonymAlgorithm},
		{"pseudonym rejects other", governance.ActionPseudonymize, governance.HashAlgorithm, true, ""},
		{"omit rejects algorithm", governance.ActionOmit, governance.HashAlgorithm, true, ""},
		{"replace rejects algorithm", governance.ActionReplace, governance.HashAlgorithm, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := base
			rule.Action, rule.Algorithm = tt.action, tt.algorithm
			if tt.action == governance.ActionPseudonymize {
				rule.KeyID = "key-1"
			}
			if tt.action == governance.ActionReplace {
				rule.Replacement = "redacted"
			}
			project.FixtureProfiles = map[string]FixtureProfile{"safe": {Rules: []GovernanceRule{rule}}}
			err := project.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected validation error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			secret := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
			policy, err := project.ResolveFixtureProfile("safe", func(string) string { return secret })
			if err != nil {
				t.Fatal(err)
			}
			if policy.Rules[0].Algorithm != tt.want {
				t.Fatalf("algorithm = %q, want %q", policy.Rules[0].Algorithm, tt.want)
			}
		})
	}
	project.FixtureProfiles = map[string]FixtureProfile{"safe": {Cohorts: []CohortPolicy{{Resource: "orders", KeyID: "key-1", Algorithm: "rank/v2", KeyPaths: []string{"pk"}, Limit: 1}}}}
	if err := project.Validate(); err == nil {
		t.Fatal("expected unsupported cohort algorithm")
	}
}

func TestDeterministicCohortRequiresExplicitFullDataMode(t *testing.T) {
	project := NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders", Data: &DynamoDBDataPolicy{Enabled: true, Mode: DataModeBounded, MaxItems: 100, MaxPages: 10}}}
	project.FixtureProfiles = map[string]FixtureProfile{"safe": {Cohorts: []CohortPolicy{{Resource: "orders", KeyID: "key-1", KeyPaths: []string{"pk"}, Limit: 10}}}}
	if err := project.Validate(); err == nil || !strings.Contains(err.Error(), "full data mode") {
		t.Fatalf("bounded cohort error = %v", err)
	}
	project.Resources.DynamoDB[0].Data = &DynamoDBDataPolicy{Enabled: true, Mode: DataModeFull}
	project.Target.HookTimeoutSeconds = 3600
	if err := project.Validate(); err != nil {
		t.Fatalf("full cohort validation = %v", err)
	}
}

func TestResolveFixtureProfileProducesStableCanonicalIdentity(t *testing.T) {
	decode := func(rules string) Project {
		project, err := Decode(strings.NewReader(`
schema_version: 1
source:
  region: eu-west-1
target:
  hook_timeout_seconds: 3600
resources:
  s3:
    - name: assets
  dynamodb:
    - name: orders
      data:
        enabled: true
        mode: full
fixture_profiles:
  safe:
    rules:
` + rules + `
    cohorts:
      - resource: orders
        key_id: fixtures-2026-08
        key_paths: [pk, sk]
        limit: 100
        predicates:
          - attribute: state
            value: active
`))
		if err != nil {
			t.Fatal(err)
		}
		return project
	}
	first := decode(`
      - id: rule-001
        service: dynamodb
        resource: orders
        target: {kind: dynamodb_attribute, path: customer.email}
        action: pseudonymize
        key_id: fixtures-2026-08
      - id: body-redaction
        service: s3
        resource: assets
        target: {kind: s3_text_body}
        action: replace
        replacement: redacted
        content_types: [text/plain, application/json]`)
	second := decode(`
      - id: body-redaction
        service: s3
        resource: assets
        target: {kind: s3_text_body}
        action: replace
        replacement: redacted
        content_types: [application/json, text/plain]
      - id: rule-001
        service: dynamodb
        resource: orders
        target: {kind: dynamodb_attribute, path: customer.email}
        action: pseudonymize
        key_id: fixtures-2026-08`)
	secret := base64.StdEncoding.EncodeToString(make([]byte, 32))
	one, err := first.ResolveFixtureProfile("safe", func(string) string { return secret })
	if err != nil {
		t.Fatal(err)
	}
	two, err := second.ResolveFixtureProfile("safe", func(string) string { return secret })
	if err != nil {
		t.Fatal(err)
	}
	if one.Identity != two.Identity {
		t.Fatalf("identities differ: %q != %q", one.Identity, two.Identity)
	}
	withoutSecret, err := first.ResolveFixtureProfile("safe", func(string) string { return "" })
	if err == nil || withoutSecret != nil {
		t.Fatal("expected the profile to require its configured secret")
	}
}

func TestValidateRejectsInvalidFixtureProfileContracts(t *testing.T) {
	valid := func() Project {
		project := NewProject()
		project.Source.Region = "eu-west-1"
		project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders"}}
		project.FixtureProfiles = map[string]FixtureProfile{"safe": {Rules: []GovernanceRule{{
			ID: "rule-001", Service: "dynamodb", Resource: "orders",
			Target: GovernanceTarget{Kind: "dynamodb_attribute", Path: "customer.email"}, Action: "omit",
		}}}}
		return project
	}
	tests := []struct {
		name   string
		mutate func(*Project)
	}{
		{"invalid profile name", func(p *Project) {
			p.FixtureProfiles["safe profile"] = p.FixtureProfiles["safe"]
			delete(p.FixtureProfiles, "safe")
		}},
		{"duplicate rule ID", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules = append(q.Rules, q.Rules[0])
			p.FixtureProfiles["safe"] = q
		}},
		{"semantic rule ID", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].ID = "customer.email"
			p.FixtureProfiles["safe"] = q
		}},
		{"duplicate target", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			r := q.Rules[0]
			r.ID = "rule-002"
			q.Rules = append(q.Rules, r)
			p.FixtureProfiles["safe"] = q
		}},
		{"unsupported action", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].Action = "mask"
			p.FixtureProfiles["safe"] = q
		}},
		{"unsupported target", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].Target.Kind = "s3_key"
			p.FixtureProfiles["safe"] = q
		}},
		{"empty replacement", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].Action = "replace"
			p.FixtureProfiles["safe"] = q
		}},
		{"missing key ID", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].Action = "pseudonymize"
			p.FixtureProfiles["safe"] = q
		}},
		{"unknown resource", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Rules[0].Resource = "customers"
			p.FixtureProfiles["safe"] = q
		}},
		{"non-positive cohort limit", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Cohorts = []CohortPolicy{{Resource: "orders", KeyID: "key-1", KeyPaths: []string{"pk"}}}
			p.FixtureProfiles["safe"] = q
		}},
		{"unsupported predicate type", func(p *Project) {
			q := p.FixtureProfiles["safe"]
			q.Cohorts = []CohortPolicy{{Resource: "orders", KeyID: "key-1", KeyPaths: []string{"pk"}, Limit: 1, Predicates: []CohortPredicate{{Attribute: "state", Value: []any{"active"}}}}}
			p.FixtureProfiles["safe"] = q
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			project := valid()
			tt.mutate(&project)
			if err := project.Validate(); err == nil {
				t.Fatal("expected fixture profile validation error")
			}
		})
	}
}

func TestResolveFixtureProfileFailsClosedForUnknownProfileAndInvalidSecret(t *testing.T) {
	project := NewProject()
	project.Source.Region = "eu-west-1"
	project.Resources.DynamoDB = []DynamoDBResource{{Name: "orders"}}
	project.FixtureProfiles = map[string]FixtureProfile{"safe": {Rules: []GovernanceRule{{
		ID: "rule-001", Service: "dynamodb", Resource: "orders",
		Target: GovernanceTarget{Kind: "dynamodb_attribute", Path: "customer.email"},
		Action: "pseudonymize", KeyID: "fixtures-2026-08",
	}}}}
	if _, err := project.ResolveFixtureProfile("missing", nil); err == nil {
		t.Fatal("expected unknown profile to fail")
	}
	for _, secret := range []string{"", "not-base64", base64.StdEncoding.EncodeToString(make([]byte, 31))} {
		if _, err := project.ResolveFixtureProfile("safe", func(string) string { return secret }); err == nil {
			t.Fatalf("expected secret %q to fail", secret)
		}
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
