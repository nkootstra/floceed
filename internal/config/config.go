package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nkootstra/floceed/internal/governance"
	"go.yaml.in/yaml/v3"
)

const CurrentSchemaVersion = 1
const DefaultFlociVersion = "1.6.0"
const DefaultPort = 4566
const DefaultHookTimeoutSeconds = 300
const DefaultCaptureResourceWorkers = 2
const DefaultReplayWorkers = 4

const (
	DefaultS3MaxObjects     = 100
	DefaultS3MaxObjectBytes = 10 << 20
	DefaultS3MaxTotalBytes  = 100 << 20
	DefaultDynamoDBMaxItems = 1000
	DefaultDynamoDBMaxPages = 100
)

type OverwritePolicy string
type DataMode string

const (
	DataModeBounded DataMode = "bounded"
	DataModeFull    DataMode = "full"
)

const (
	OverwriteIfDifferent OverwritePolicy = "if-different"
	OverwriteAlways      OverwritePolicy = "always"
	OverwriteNever       OverwritePolicy = "never"
)

type Project struct {
	SchemaVersion   int                       `yaml:"schema_version" json:"schema_version"`
	Source          Source                    `yaml:"source" json:"source"`
	Target          Target                    `yaml:"target,omitempty" json:"target"`
	Resources       Resources                 `yaml:"resources,omitempty" json:"resources"`
	Capture         Capture                   `yaml:"capture,omitempty" json:"capture"`
	Output          Output                    `yaml:"output,omitempty" json:"output"`
	FixtureProfiles map[string]FixtureProfile `yaml:"fixture_profiles,omitempty" json:"fixture_profiles,omitempty"`
}

type FixtureProfile struct {
	Rules   []GovernanceRule `yaml:"rules,omitempty" json:"rules,omitempty"`
	Cohorts []CohortPolicy   `yaml:"cohorts,omitempty" json:"cohorts,omitempty"`
}

type GovernanceTarget struct {
	Kind governance.TargetKind `yaml:"kind" json:"kind"`
	Path string                `yaml:"path,omitempty" json:"path,omitempty"`
}

type GovernanceRule struct {
	ID           string             `yaml:"id" json:"id"`
	Service      governance.Service `yaml:"service" json:"service"`
	Resource     string             `yaml:"resource" json:"resource"`
	Target       GovernanceTarget   `yaml:"target" json:"target"`
	Action       governance.Action  `yaml:"action" json:"action"`
	Replacement  string             `yaml:"replacement,omitempty" json:"replacement,omitempty"`
	KeyID        string             `yaml:"key_id,omitempty" json:"key_id,omitempty"`
	Scope        string             `yaml:"scope,omitempty" json:"scope,omitempty"`
	Algorithm    string             `yaml:"algorithm,omitempty" json:"algorithm,omitempty"`
	ContentTypes []string           `yaml:"content_types,omitempty" json:"content_types,omitempty"`
}

type CohortPredicate struct {
	Attribute string `yaml:"attribute" json:"attribute"`
	Value     any    `yaml:"value" json:"value"`
}

type CohortPolicy struct {
	Resource         string            `yaml:"resource" json:"resource"`
	KeyID            string            `yaml:"key_id" json:"key_id"`
	Scope            string            `yaml:"scope,omitempty" json:"scope,omitempty"`
	Algorithm        string            `yaml:"algorithm,omitempty" json:"algorithm,omitempty"`
	KeyPaths         []string          `yaml:"key_paths" json:"key_paths"`
	Limit            int               `yaml:"limit" json:"limit"`
	MaxRetainedBytes int64             `yaml:"max_retained_bytes,omitempty" json:"max_retained_bytes,omitempty"`
	Predicates       []CohortPredicate `yaml:"predicates,omitempty" json:"predicates,omitempty"`
}
type Source struct {
	Profile           string `yaml:"profile,omitempty" json:"profile,omitempty"`
	Region            string `yaml:"region" json:"region"`
	ExpectedAccountID string `yaml:"expected_account_id,omitempty" json:"expected_account_id,omitempty"`
}
type Target struct {
	FlociVersion       string      `yaml:"floci_version,omitempty" json:"floci_version"`
	Port               int         `yaml:"port,omitempty" json:"port"`
	HookTimeoutSeconds int         `yaml:"hook_timeout_seconds,omitempty" json:"hook_timeout_seconds"`
	ReplayWorkers      int         `yaml:"replay_workers,omitempty" json:"replay_workers"`
	Persistence        Persistence `yaml:"persistence,omitempty" json:"persistence"`
}
type Persistence struct {
	Enabled bool   `yaml:"enabled,omitempty" json:"enabled"`
	Volume  string `yaml:"volume,omitempty" json:"volume,omitempty"`
	Path    string `yaml:"path,omitempty" json:"path,omitempty"`
}
type Capture struct {
	AllowPartialData bool `yaml:"allow_partial_data,omitempty" json:"allow_partial_data"`
	ResourceWorkers  int  `yaml:"resource_workers,omitempty" json:"resource_workers"`
}
type Output struct {
	Directory string `yaml:"directory,omitempty" json:"directory"`
}
type Resources struct {
	S3            []S3Resource           `yaml:"s3,omitempty" json:"s3"`
	DynamoDB      []DynamoDBResource     `yaml:"dynamodb,omitempty" json:"dynamodb"`
	SQS           []SQSResource          `yaml:"sqs,omitempty" json:"sqs"`
	SNS           []SNSResource          `yaml:"sns,omitempty" json:"sns"`
	Kinesis       []KinesisResource      `yaml:"kinesis,omitempty" json:"kinesis"`
	EventBridge   []EventBridgeResource  `yaml:"eventbridge,omitempty" json:"eventbridge"`
	Lambda        []LambdaResource       `yaml:"lambda,omitempty" json:"lambda"`
	Secrets       []SecretResource       `yaml:"secrets,omitempty" json:"secrets"`
	Parameters    []ParameterResource    `yaml:"parameters,omitempty" json:"parameters"`
	APIs          []APIResource          `yaml:"api_gateway,omitempty" json:"api_gateway"`
	StateMachines []StateMachineResource `yaml:"step_functions,omitempty" json:"step_functions"`
	LogGroups     []LogGroupResource     `yaml:"cloudwatch_logs,omitempty" json:"cloudwatch_logs"`
}
type S3Resource struct {
	Name string        `yaml:"name" json:"name"`
	Data *S3DataPolicy `yaml:"data,omitempty" json:"data,omitempty"`
}
type S3DataPolicy struct {
	Enabled        bool            `yaml:"enabled" json:"enabled"`
	Mode           DataMode        `yaml:"mode,omitempty" json:"mode"`
	Prefixes       []string        `yaml:"prefixes,omitempty" json:"prefixes,omitempty"`
	MaxObjects     int             `yaml:"max_objects,omitempty" json:"max_objects,omitempty"`
	MaxObjectBytes int64           `yaml:"max_object_bytes,omitempty" json:"max_object_bytes,omitempty"`
	MaxTotalBytes  int64           `yaml:"max_total_bytes,omitempty" json:"max_total_bytes,omitempty"`
	Overwrite      OverwritePolicy `yaml:"overwrite,omitempty" json:"overwrite"`
}
type DynamoDBResource struct {
	Name                string              `yaml:"name" json:"name"`
	PreserveProvisioned bool                `yaml:"preserve_provisioned,omitempty" json:"preserve_provisioned"`
	Data                *DynamoDBDataPolicy `yaml:"data,omitempty" json:"data,omitempty"`
}
type SQSResource struct {
	Name string         `yaml:"name" json:"name"`
	ARN  string         `yaml:"arn" json:"arn"`
	Data *SQSDataPolicy `yaml:"data,omitempty" json:"data,omitempty"`
}
type SNSResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type KinesisResource struct {
	Name string             `yaml:"name" json:"name"`
	ARN  string             `yaml:"arn" json:"arn"`
	Data *KinesisDataPolicy `yaml:"data,omitempty" json:"data,omitempty"`
}
type EventBridgeResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type LambdaResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type SecretResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type ParameterResource struct {
	Name           string `yaml:"name" json:"name"`
	ARN            string `yaml:"arn" json:"arn"`
	WithDecryption bool   `yaml:"with_decryption,omitempty" json:"with_decryption"`
}
type APIResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type StateMachineResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type LogGroupResource struct {
	Name string `yaml:"name" json:"name"`
	ARN  string `yaml:"arn" json:"arn"`
}
type DynamoDBDataPolicy struct {
	Enabled  bool     `yaml:"enabled" json:"enabled"`
	Mode     DataMode `yaml:"mode,omitempty" json:"mode"`
	MaxItems int      `yaml:"max_items,omitempty" json:"max_items,omitempty"`
	MaxPages int      `yaml:"max_pages,omitempty" json:"max_pages,omitempty"`
	Gzip     *bool    `yaml:"gzip,omitempty" json:"gzip,omitempty"`
}

type KinesisDataPolicy struct {
	Enabled    bool     `yaml:"enabled" json:"enabled"`
	Mode       DataMode `yaml:"mode,omitempty" json:"mode"`
	MaxRecords int      `yaml:"max_records,omitempty" json:"max_records,omitempty"`
	MaxBytes   int64    `yaml:"max_bytes,omitempty" json:"max_bytes,omitempty"`
}

type SQSDataPolicy struct {
	Enabled     bool     `yaml:"enabled" json:"enabled"`
	Mode        DataMode `yaml:"mode,omitempty" json:"mode"`
	MaxMessages int      `yaml:"max_messages,omitempty" json:"max_messages,omitempty"`
	MaxBytes    int64    `yaml:"max_bytes,omitempty" json:"max_bytes,omitempty"`
}

func NewSQSDataPolicy() *SQSDataPolicy {
	return &SQSDataPolicy{Enabled: true, Mode: DataModeBounded, MaxMessages: 100, MaxBytes: 16 << 20}
}

func NewKinesisDataPolicy() *KinesisDataPolicy {
	return &KinesisDataPolicy{Enabled: true, Mode: DataModeBounded, MaxRecords: 1000, MaxBytes: 64 << 20}
}

func NewS3DataPolicy() *S3DataPolicy {
	return &S3DataPolicy{
		Enabled:        true,
		Mode:           DataModeBounded,
		MaxObjects:     DefaultS3MaxObjects,
		MaxObjectBytes: DefaultS3MaxObjectBytes,
		MaxTotalBytes:  DefaultS3MaxTotalBytes,
		Overwrite:      OverwriteIfDifferent,
	}
}

func NewDynamoDBDataPolicy() *DynamoDBDataPolicy {
	gzipEnabled := true
	return &DynamoDBDataPolicy{
		Enabled:  true,
		Mode:     DataModeBounded,
		MaxItems: DefaultDynamoDBMaxItems,
		MaxPages: DefaultDynamoDBMaxPages,
		Gzip:     &gzipEnabled,
	}
}

var accountID = regexp.MustCompile(`^[0-9]{12}$`)
var opaqueName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
var dependencyBaseName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
var arnPartition = regexp.MustCompile(`^(?:aws|aws-us-gov|aws-cn|aws-iso|aws-iso-b|aws-iso-e|aws-iso-f|aws-eusc)$`)

func Decode(r io.Reader) (Project, error) {
	p := NewProject()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return Project{}, fmt.Errorf("decode floceed project: %w", err)
	}
	p.applyDefaults()
	if err := p.Validate(); err != nil {
		return Project{}, err
	}
	return p, nil
}

func NewProject() Project {
	return Project{
		SchemaVersion: CurrentSchemaVersion,
		Target: Target{
			FlociVersion:       DefaultFlociVersion,
			Port:               DefaultPort,
			HookTimeoutSeconds: DefaultHookTimeoutSeconds,
			ReplayWorkers:      DefaultReplayWorkers,
		},
		Capture: Capture{ResourceWorkers: DefaultCaptureResourceWorkers},
		Output:  Output{Directory: ".floceed"},
	}
}

// ResolveFixtureProfile compiles a named project profile into an immutable,
// normalized runtime policy. An empty name preserves legacy capture behavior.
func (p Project) ResolveFixtureProfile(name string, getenv func(string) string) (*governance.EffectivePolicy, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		if len(p.FixtureProfiles) != 0 {
			return nil, fmt.Errorf("fixture profile must be selected when fixture_profiles are configured: %w", ErrValidation)
		}
		return nil, nil
	}
	profile, ok := p.FixtureProfiles[name]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownFixtureProfile, name)
	}
	rules := make([]governance.Rule, len(profile.Rules))
	keyed := len(profile.Cohorts) != 0
	for i, rule := range profile.Rules {
		rules[i] = governance.Rule{
			ID: rule.ID, Service: rule.Service, Resource: rule.Resource,
			Target: governance.Target{Kind: rule.Target.Kind, Path: rule.Target.Path},
			Action: rule.Action, Replacement: rule.Replacement, KeyID: rule.KeyID,
			Scope: rule.Scope, Algorithm: rule.Algorithm, ContentTypes: append([]string(nil), rule.ContentTypes...),
		}
		keyed = keyed || rule.Action == governance.ActionPseudonymize
	}
	cohorts := make([]governance.Cohort, len(profile.Cohorts))
	for i, cohort := range profile.Cohorts {
		predicates := make([]governance.Predicate, len(cohort.Predicates))
		for j, predicate := range cohort.Predicates {
			predicates[j] = governance.Predicate{Attribute: predicate.Attribute, Value: predicate.Value}
		}
		cohorts[i] = governance.Cohort{Resource: cohort.Resource, KeyID: cohort.KeyID, Scope: cohort.Scope, Algorithm: cohort.Algorithm, KeyPaths: append([]string(nil), cohort.KeyPaths...), Limit: cohort.Limit, MaxRetainedBytes: cohort.MaxRetainedBytes, Predicates: predicates}
	}
	var secret []byte
	if keyed {
		if getenv == nil {
			getenv = func(string) string { return "" }
		}
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(getenv("FLOCEED_GOVERNANCE_SECRET")))
		if err != nil || len(decoded) < 32 {
			return nil, fmt.Errorf("FLOCEED_GOVERNANCE_SECRET must be base64 for at least 32 bytes: %w", ErrValidation)
		}
		secret = decoded
	}
	return governance.NewEffectivePolicy(name, rules, cohorts, secret)
}

func (p *Project) applyDefaults() {
	if p.Target.FlociVersion == "" {
		p.Target.FlociVersion = DefaultFlociVersion
	}
	if p.Target.Port == 0 {
		p.Target.Port = DefaultPort
	}
	if p.Target.HookTimeoutSeconds == 0 {
		p.Target.HookTimeoutSeconds = DefaultHookTimeoutSeconds
	}
	if p.Target.ReplayWorkers == 0 {
		p.Target.ReplayWorkers = DefaultReplayWorkers
	}
	if p.Capture.ResourceWorkers == 0 {
		p.Capture.ResourceWorkers = DefaultCaptureResourceWorkers
	}
	if p.Output.Directory == "" {
		p.Output.Directory = ".floceed"
	}
	for i := range p.Resources.S3 {
		if p.Resources.S3[i].Data != nil && p.Resources.S3[i].Data.Mode == "" {
			p.Resources.S3[i].Data.Mode = DataModeBounded
		}
		if p.Resources.S3[i].Data != nil && p.Resources.S3[i].Data.Overwrite == "" {
			p.Resources.S3[i].Data.Overwrite = OverwriteIfDifferent
		}
	}
	for i := range p.Resources.DynamoDB {
		if p.Resources.DynamoDB[i].Data != nil && p.Resources.DynamoDB[i].Data.Mode == "" {
			p.Resources.DynamoDB[i].Data.Mode = DataModeBounded
		}
	}
	for i := range p.Resources.Kinesis {
		if p.Resources.Kinesis[i].Data != nil {
			if p.Resources.Kinesis[i].Data.Mode == "" {
				p.Resources.Kinesis[i].Data.Mode = DataModeBounded
			}
			if p.Resources.Kinesis[i].Data.MaxRecords == 0 {
				p.Resources.Kinesis[i].Data.MaxRecords = 1000
			}
			if p.Resources.Kinesis[i].Data.MaxBytes == 0 {
				p.Resources.Kinesis[i].Data.MaxBytes = 64 << 20
			}
		}
	}
	for i := range p.Resources.SQS {
		if p.Resources.SQS[i].Data != nil {
			if p.Resources.SQS[i].Data.Mode == "" {
				p.Resources.SQS[i].Data.Mode = DataModeBounded
			}
			if p.Resources.SQS[i].Data.MaxMessages == 0 {
				p.Resources.SQS[i].Data.MaxMessages = 100
			}
			if p.Resources.SQS[i].Data.MaxBytes == 0 {
				p.Resources.SQS[i].Data.MaxBytes = 16 << 20
			}
		}
	}
	for name, profile := range p.FixtureProfiles {
		for i := range profile.Cohorts {
			if profile.Cohorts[i].MaxRetainedBytes == 0 {
				profile.Cohorts[i].MaxRetainedBytes = governance.DefaultCohortMaxRetainedBytes
			}
		}
		p.FixtureProfiles[name] = profile
	}
}
func (p Project) Validate() error {
	if p.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("unsupported project schema %d: %w", p.SchemaVersion, ErrValidation)
	}
	if strings.TrimSpace(p.Source.Region) == "" {
		return fmt.Errorf("source.region is required: %w", ErrValidation)
	}
	if p.Source.ExpectedAccountID != "" && !accountID.MatchString(p.Source.ExpectedAccountID) {
		return fmt.Errorf("source.expected_account_id must be 12 digits: %w", ErrValidation)
	}
	if p.Target.FlociVersion != "" && p.Target.FlociVersion != DefaultFlociVersion {
		return fmt.Errorf("target Floci %q is not supported: %w", p.Target.FlociVersion, ErrValidation)
	}
	if p.Target.Port < 0 || p.Target.Port > 65535 {
		return fmt.Errorf("target.port is invalid: %w", ErrValidation)
	}
	if p.Target.ReplayWorkers < 0 || p.Target.ReplayWorkers > 32 {
		return fmt.Errorf("target.replay_workers must be between 0 and 32 (0 uses the default of %d): %w", DefaultReplayWorkers, ErrValidation)
	}
	if p.Capture.ResourceWorkers < 0 || p.Capture.ResourceWorkers > 8 {
		return fmt.Errorf("capture.resource_workers must be between 0 and 8 (0 uses the default of %d): %w", DefaultCaptureResourceWorkers, ErrValidation)
	}
	if p.Output.Directory != "" {
		clean := filepath.ToSlash(filepath.Clean(p.Output.Directory))
		if filepath.IsAbs(p.Output.Directory) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(p.Output.Directory, `\`) {
			return fmt.Errorf("output.directory must be a safe relative path: %w", ErrValidation)
		}
	}
	s3Names := make([]string, len(p.Resources.S3))
	for i, r := range p.Resources.S3 {
		s3Names[i] = r.Name
	}
	if err := validateResourceNames("S3", s3Names); err != nil {
		return err
	}
	for _, r := range p.Resources.S3 {
		if d := r.Data; d != nil && d.Enabled {
			mode := d.Mode
			if mode == "" {
				mode = DataModeBounded
			}
			if mode != DataModeBounded && mode != DataModeFull {
				return fmt.Errorf("S3 %q has invalid data mode %q: %w", r.Name, d.Mode, ErrValidation)
			}
			if mode == DataModeBounded && (d.MaxObjects <= 0 || d.MaxObjectBytes <= 0 || d.MaxTotalBytes <= 0) {
				return fmt.Errorf("S3 %q data requires positive object and byte limits: %w", r.Name, ErrValidation)
			}
			if mode == DataModeFull && (d.MaxObjects != 0 || d.MaxObjectBytes != 0 || d.MaxTotalBytes != 0) {
				return fmt.Errorf("S3 %q full data mode cannot set bounded limits: %w", r.Name, ErrValidation)
			}
			if !d.Overwrite.valid() {
				return fmt.Errorf("S3 %q has invalid overwrite policy: %w", r.Name, ErrValidation)
			}
		}
	}
	dynamoDBNames := make([]string, len(p.Resources.DynamoDB))
	for i, r := range p.Resources.DynamoDB {
		dynamoDBNames[i] = r.Name
	}
	if err := validateResourceNames("DynamoDB", dynamoDBNames); err != nil {
		return err
	}
	for _, r := range p.Resources.DynamoDB {
		if d := r.Data; d != nil && d.Enabled {
			mode := d.Mode
			if mode == "" {
				mode = DataModeBounded
			}
			if mode != DataModeBounded && mode != DataModeFull {
				return fmt.Errorf("DynamoDB %q has invalid data mode %q: %w", r.Name, d.Mode, ErrValidation)
			}
			if mode == DataModeBounded && (d.MaxItems <= 0 || d.MaxPages <= 0) {
				return fmt.Errorf("DynamoDB %q data requires positive item and page limits: %w", r.Name, ErrValidation)
			}
			if mode == DataModeFull && (d.MaxItems != 0 || d.MaxPages != 0) {
				return fmt.Errorf("DynamoDB %q full data mode cannot set bounded limits: %w", r.Name, ErrValidation)
			}
		}
	}
	if err := validateSQSResources(p.Resources.SQS); err != nil {
		return err
	}
	if err := validateSNSResources(p.Resources.SNS); err != nil {
		return err
	}
	if err := validateKinesisResources(p.Resources.Kinesis); err != nil {
		return err
	}
	if err := validateEventBridgeResources(p.Resources.EventBridge); err != nil {
		return err
	}
	if err := validateLambdaResources(p.Resources.Lambda); err != nil {
		return err
	}
	if err := validateSecretResources(p.Resources.Secrets); err != nil {
		return err
	}
	if err := validateParameterResources(p.Resources.Parameters); err != nil {
		return err
	}
	if err := validateAPIResources(p.Resources.APIs); err != nil {
		return err
	}
	if err := validateStateMachineResources(p.Resources.StateMachines); err != nil {
		return err
	}
	if err := validateLogGroupResources(p.Resources.LogGroups); err != nil {
		return err
	}
	if hasFullData(p) && p.Target.HookTimeoutSeconds <= DefaultHookTimeoutSeconds {
		return fmt.Errorf("full data mode requires target.hook_timeout_seconds greater than %d: %w", DefaultHookTimeoutSeconds, ErrValidation)
	}
	if err := p.validateFixtureProfiles(); err != nil {
		return err
	}
	return nil
}

func validateSQSResources(resources []SQSResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if !validDependencyName(resource.Name, 80) {
			return fmt.Errorf("SQS resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		if err := validateDependencyARN("sqs", resource.Name, resource.ARN); err != nil {
			return fmt.Errorf("SQS resource %q: %w", resource.Name, err)
		}
		if resource.Data != nil && (resource.Data.Mode != DataModeBounded || resource.Data.MaxMessages < 0 || resource.Data.MaxBytes < 0) {
			return fmt.Errorf("SQS resource %q has invalid data policy: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("SQS", names)
}

func validateSNSResources(resources []SNSResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if !validDependencyName(resource.Name, 256) {
			return fmt.Errorf("SNS resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		if err := validateDependencyARN("sns", resource.Name, resource.ARN); err != nil {
			return fmt.Errorf("SNS resource %q: %w", resource.Name, err)
		}
	}
	return validateResourceNames("SNS", names)
}

func validateKinesisResources(resources []KinesisResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if !validDependencyName(resource.Name, 128) || strings.HasSuffix(resource.Name, ".fifo") {
			return fmt.Errorf("Kinesis resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		parts := strings.Split(resource.ARN, ":")
		if len(parts) != 6 || parts[0] != "arn" || !arnPartition.MatchString(parts[1]) || parts[2] != "kinesis" || parts[3] == "" || !accountID.MatchString(parts[4]) || parts[5] != "stream/"+resource.Name {
			return fmt.Errorf("Kinesis resource %q: ARN %q does not match stream name: %w", resource.Name, resource.ARN, ErrValidation)
		}
		if resource.Data != nil {
			if resource.Data.Mode != DataModeBounded && resource.Data.Mode != DataModeFull {
				return fmt.Errorf("Kinesis resource %q has invalid data.mode: %w", resource.Name, ErrValidation)
			}
			if resource.Data.MaxRecords < 0 || resource.Data.MaxBytes < 0 {
				return fmt.Errorf("Kinesis resource %q has invalid data limits: %w", resource.Name, ErrValidation)
			}
		}
	}
	return validateResourceNames("Kinesis", names)
}

func validateEventBridgeResources(resources []EventBridgeResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if !validDependencyName(resource.Name, 256) || resource.Name == "default" {
			return fmt.Errorf("EventBridge resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		parts := strings.Split(resource.ARN, ":")
		if len(parts) != 6 || parts[0] != "arn" || !arnPartition.MatchString(parts[1]) || parts[2] != "events" || parts[3] == "" || !accountID.MatchString(parts[4]) || parts[5] != "event-bus/"+resource.Name {
			return fmt.Errorf("EventBridge resource %q: ARN %q does not match event bus name: %w", resource.Name, resource.ARN, ErrValidation)
		}
	}
	return validateResourceNames("EventBridge", names)
}

func validateLambdaResources(resources []LambdaResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if !validDependencyName(resource.Name, 64) {
			return fmt.Errorf("Lambda resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		parts := strings.Split(resource.ARN, ":")
		if len(parts) != 7 || parts[0] != "arn" || !arnPartition.MatchString(parts[1]) || parts[2] != "lambda" || parts[3] == "" || !accountID.MatchString(parts[4]) || parts[5] != "function" || parts[6] != resource.Name {
			return fmt.Errorf("Lambda resource %q: ARN %q does not match function name: %w", resource.Name, resource.ARN, ErrValidation)
		}
	}
	return validateResourceNames("Lambda", names)
}

func validateSecretResources(resources []SecretResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if resource.Name == "" || len(resource.Name) > 512 {
			return fmt.Errorf("Secrets Manager resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		parts := strings.Split(resource.ARN, ":")
		// Secrets Manager ARNs carry the resource as `secret:<name>-<suffix>`,
		// which splits into two segments: "secret" and the name with its random
		// suffix. Accept only the bare name or the name plus AWS's fixed
		// `-XXXXXX` suffix, so a different secret whose name merely starts with
		// this one cannot pass.
		if len(parts) != 7 || parts[0] != "arn" || !arnPartition.MatchString(parts[1]) || parts[2] != "secretsmanager" || parts[3] == "" || !accountID.MatchString(parts[4]) || parts[5] != "secret" || !secretResourceMatchesName(parts[6], resource.Name) {
			return fmt.Errorf("Secrets Manager resource %q has invalid ARN: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("Secrets Manager", names)
}

// secretResourceMatchesName reports whether the resource part of a Secrets
// Manager ARN names the configured secret. AWS appends a random `-XXXXXX`
// suffix to the secret name, so the bare name and the name plus that exact
// suffix are accepted, but a name that merely shares a prefix is not.
func secretResourceMatchesName(resource, name string) bool {
	if resource == name {
		return true
	}
	if !strings.HasPrefix(resource, name+"-") {
		return false
	}
	suffix := strings.TrimPrefix(resource, name+"-")
	if len(suffix) != 6 {
		return false
	}
	for _, r := range suffix {
		if !alphanumeric(r) {
			return false
		}
	}
	return true
}

func alphanumeric(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9'
}

func validateParameterResources(resources []ParameterResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if resource.Name == "" || len(resource.Name) > 2048 || !strings.HasPrefix(resource.Name, "/") {
			return fmt.Errorf("SSM parameter %q has invalid name: %w", resource.Name, ErrValidation)
		}
		parts := strings.Split(resource.ARN, ":")
		if len(parts) != 6 || parts[2] != "ssm" || parts[5] != "parameter"+resource.Name || !accountID.MatchString(parts[4]) {
			return fmt.Errorf("SSM parameter %q has invalid ARN: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("SSM parameters", names)
}

func validateAPIResources(resources []APIResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if resource.Name == "" || len(resource.Name) > 128 {
			return fmt.Errorf("API Gateway resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		if resource.ARN == "" || !strings.Contains(resource.ARN, ":apigateway:") {
			return fmt.Errorf("API Gateway resource %q has invalid ARN: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("API Gateway", names)
}

func validateStateMachineResources(resources []StateMachineResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if resource.Name == "" || len(resource.Name) > 80 {
			return fmt.Errorf("Step Functions resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		if resource.ARN == "" || !strings.Contains(resource.ARN, ":states:") || !strings.Contains(resource.ARN, ":stateMachine:") {
			return fmt.Errorf("Step Functions resource %q has invalid ARN: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("Step Functions", names)
}

func validateLogGroupResources(resources []LogGroupResource) error {
	names := make([]string, len(resources))
	for i, resource := range resources {
		names[i] = resource.Name
		if resource.Name == "" || len(resource.Name) > 512 {
			return fmt.Errorf("CloudWatch Logs resource %q has invalid name: %w", resource.Name, ErrValidation)
		}
		if resource.ARN == "" || !strings.Contains(resource.ARN, ":logs:") || !strings.Contains(resource.ARN, ":log-group:") {
			return fmt.Errorf("CloudWatch Logs resource %q has invalid ARN: %w", resource.Name, ErrValidation)
		}
	}
	return validateResourceNames("CloudWatch Logs", names)
}

func validDependencyName(name string, max int) bool {
	if len(name) > max || name == "" {
		return false
	}
	base := strings.TrimSuffix(name, ".fifo")
	return dependencyBaseName.MatchString(base) && (base == name || strings.HasSuffix(name, ".fifo"))
}

func validateDependencyARN(service, name, arn string) error {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || !arnPartition.MatchString(parts[1]) || parts[2] != service || parts[3] == "" || !accountID.MatchString(parts[4]) || parts[5] != name {
		return fmt.Errorf("ARN %q does not match %s resource name: %w", arn, service, ErrValidation)
	}
	return nil
}

func (p Project) validateFixtureProfiles() error {
	resources := map[governance.Service]map[string]bool{
		governance.ServiceS3: {}, governance.ServiceDynamoDB: {},
	}
	dynamoFull := make(map[string]bool)
	for _, resource := range p.Resources.S3 {
		resources[governance.ServiceS3][resource.Name] = true
	}
	for _, resource := range p.Resources.DynamoDB {
		resources[governance.ServiceDynamoDB][resource.Name] = true
		dynamoFull[resource.Name] = resource.Data != nil && resource.Data.Enabled && resource.Data.Mode == DataModeFull
	}
	for name, profile := range p.FixtureProfiles {
		if !opaqueName.MatchString(name) {
			return fmt.Errorf("fixture profile name %q must be an opaque identifier: %w", name, ErrValidation)
		}
		ids := make(map[string]bool)
		targets := make(map[string]bool)
		for _, rule := range profile.Rules {
			if !opaqueName.MatchString(rule.ID) {
				return fmt.Errorf("fixture profile %q rule ID %q must be an opaque identifier: %w", name, rule.ID, ErrValidation)
			}
			if ids[rule.ID] {
				return fmt.Errorf("fixture profile %q has duplicate rule ID %q: %w", name, rule.ID, ErrValidation)
			}
			ids[rule.ID] = true
			if !resources[rule.Service][rule.Resource] {
				return fmt.Errorf("fixture profile %q rule %q names unconfigured %s resource %q: %w", name, rule.ID, rule.Service, rule.Resource, ErrValidation)
			}
			if err := validateGovernanceRule(rule); err != nil {
				return fmt.Errorf("fixture profile %q rule %q: %w", name, rule.ID, err)
			}
			path := strings.TrimSpace(rule.Target.Path)
			if rule.Target.Kind == governance.TargetS3Metadata {
				path = strings.ToLower(path)
			}
			targetKey := string(rule.Service) + "\x00" + rule.Resource + "\x00" + string(rule.Target.Kind) + "\x00" + path
			if targets[targetKey] {
				return fmt.Errorf("fixture profile %q has overlapping rules for one target: %w", name, ErrValidation)
			}
			targets[targetKey] = true
		}
		cohortResources := make(map[string]bool)
		for _, cohort := range profile.Cohorts {
			if !resources[governance.ServiceDynamoDB][cohort.Resource] {
				return fmt.Errorf("fixture profile %q cohort names unconfigured DynamoDB resource %q: %w", name, cohort.Resource, ErrValidation)
			}
			if !dynamoFull[cohort.Resource] {
				return fmt.Errorf("fixture profile %q cohort %q requires explicit DynamoDB full data mode: %w", name, cohort.Resource, ErrValidation)
			}
			if cohortResources[cohort.Resource] {
				return fmt.Errorf("fixture profile %q has duplicate cohort for %q: %w", name, cohort.Resource, ErrValidation)
			}
			cohortResources[cohort.Resource] = true
			if strings.TrimSpace(cohort.KeyID) == "" || cohort.Limit <= 0 || len(cohort.KeyPaths) == 0 {
				return fmt.Errorf("fixture profile %q cohort %q requires key_id, key_paths, and a positive limit: %w", name, cohort.Resource, ErrValidation)
			}
			if cohort.MaxRetainedBytes < 0 {
				return fmt.Errorf("fixture profile %q cohort %q requires a positive max_retained_bytes: %w", name, cohort.Resource, ErrValidation)
			}
			if algorithm := strings.TrimSpace(cohort.Algorithm); algorithm != "" && algorithm != governance.CohortRankAlgorithm {
				return fmt.Errorf("fixture profile %q cohort %q requires algorithm %q: %w", name, cohort.Resource, governance.CohortRankAlgorithm, ErrValidation)
			}
			for _, path := range cohort.KeyPaths {
				if strings.TrimSpace(path) == "" {
					return fmt.Errorf("fixture profile %q cohort %q has an empty key path: %w", name, cohort.Resource, ErrValidation)
				}
			}
			for _, predicate := range cohort.Predicates {
				if strings.TrimSpace(predicate.Attribute) == "" || !supportedPredicateValue(predicate.Value) {
					return fmt.Errorf("fixture profile %q cohort %q has an unsupported predicate: %w", name, cohort.Resource, ErrValidation)
				}
			}
		}
	}
	return nil
}

func validateGovernanceRule(rule GovernanceRule) error {
	validAction := rule.Action == governance.ActionOmit || rule.Action == governance.ActionReplace || rule.Action == governance.ActionHash || rule.Action == governance.ActionPseudonymize
	if !validAction {
		return fmt.Errorf("unsupported action %q: %w", rule.Action, ErrValidation)
	}
	path := strings.TrimSpace(rule.Target.Path)
	switch {
	case rule.Service == governance.ServiceDynamoDB && rule.Target.Kind == governance.TargetDynamoDBAttribute && path != "":
	case rule.Service == governance.ServiceS3 && rule.Target.Kind == governance.TargetS3Metadata && path != "":
	case rule.Service == governance.ServiceS3 && rule.Target.Kind == governance.TargetS3TextBody && path == "" && len(rule.ContentTypes) != 0:
	default:
		return fmt.Errorf("unsupported target %q for service %q: %w", rule.Target.Kind, rule.Service, ErrValidation)
	}
	if rule.Action == governance.ActionReplace && rule.Replacement == "" {
		return fmt.Errorf("replace requires a non-empty replacement: %w", ErrValidation)
	}
	if rule.Action == governance.ActionPseudonymize && strings.TrimSpace(rule.KeyID) == "" {
		return fmt.Errorf("pseudonymize requires key_id: %w", ErrValidation)
	}
	algorithm := strings.TrimSpace(rule.Algorithm)
	switch rule.Action {
	case governance.ActionHash:
		if algorithm != "" && algorithm != governance.HashAlgorithm {
			return fmt.Errorf("hash requires algorithm %q: %w", governance.HashAlgorithm, ErrValidation)
		}
	case governance.ActionPseudonymize:
		if algorithm != "" && algorithm != governance.PseudonymAlgorithm {
			return fmt.Errorf("pseudonymize requires algorithm %q: %w", governance.PseudonymAlgorithm, ErrValidation)
		}
	default:
		if algorithm != "" {
			return fmt.Errorf("%s does not accept an algorithm: %w", rule.Action, ErrValidation)
		}
	}
	return nil
}

func supportedPredicateValue(value any) bool {
	switch value.(type) {
	case string, bool, int, int64, uint64, float64:
		return true
	default:
		return false
	}
}

func hasFullData(p Project) bool {
	for _, r := range p.Resources.S3 {
		if r.Data != nil && r.Data.Enabled && r.Data.Mode == DataModeFull {
			return true
		}
	}
	for _, r := range p.Resources.DynamoDB {
		if r.Data != nil && r.Data.Enabled && r.Data.Mode == DataModeFull {
			return true
		}
	}
	return false
}

func (p OverwritePolicy) valid() bool {
	switch p {
	case "", OverwriteIfDifferent, OverwriteAlways, OverwriteNever:
		return true
	default:
		return false
	}
}

func validateResourceNames(service string, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%s name is required: %w", service, ErrValidation)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate %s resource %q: %w", service, name, ErrValidation)
		}
		seen[name] = struct{}{}
	}
	return nil
}

var ErrValidation = errors.New("invalid floceed project")
var ErrUnknownFixtureProfile = errors.New("unknown fixture profile")
