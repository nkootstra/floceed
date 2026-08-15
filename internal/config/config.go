package config

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

const CurrentSchemaVersion = 1
const DefaultFlociVersion = "1.6.0"
const DefaultPort = 4566
const DefaultHookTimeoutSeconds = 300

const (
	DefaultS3MaxObjects     = 100
	DefaultS3MaxObjectBytes = 10 << 20
	DefaultS3MaxTotalBytes  = 100 << 20
	DefaultDynamoDBMaxItems = 1000
	DefaultDynamoDBMaxPages = 100
)

type OverwritePolicy string

const (
	OverwriteIfDifferent OverwritePolicy = "if-different"
	OverwriteAlways      OverwritePolicy = "always"
	OverwriteNever       OverwritePolicy = "never"
)

type Project struct {
	SchemaVersion int       `yaml:"schema_version" json:"schema_version"`
	Source        Source    `yaml:"source" json:"source"`
	Target        Target    `yaml:"target,omitempty" json:"target"`
	Resources     Resources `yaml:"resources,omitempty" json:"resources"`
	Capture       Capture   `yaml:"capture,omitempty" json:"capture"`
	Output        Output    `yaml:"output,omitempty" json:"output"`
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
	Persistence        Persistence `yaml:"persistence,omitempty" json:"persistence"`
}
type Persistence struct {
	Enabled bool   `yaml:"enabled,omitempty" json:"enabled"`
	Volume  string `yaml:"volume,omitempty" json:"volume,omitempty"`
	Path    string `yaml:"path,omitempty" json:"path,omitempty"`
}
type Capture struct {
	AllowPartialData bool `yaml:"allow_partial_data,omitempty" json:"allow_partial_data"`
}
type Output struct {
	Directory string `yaml:"directory,omitempty" json:"directory"`
}
type Resources struct {
	S3       []S3Resource       `yaml:"s3,omitempty" json:"s3"`
	DynamoDB []DynamoDBResource `yaml:"dynamodb,omitempty" json:"dynamodb"`
}
type S3Resource struct {
	Name string        `yaml:"name" json:"name"`
	Data *S3DataPolicy `yaml:"data,omitempty" json:"data,omitempty"`
}
type S3DataPolicy struct {
	Enabled        bool            `yaml:"enabled" json:"enabled"`
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
type DynamoDBDataPolicy struct {
	Enabled  bool  `yaml:"enabled" json:"enabled"`
	MaxItems int   `yaml:"max_items,omitempty" json:"max_items,omitempty"`
	MaxPages int   `yaml:"max_pages,omitempty" json:"max_pages,omitempty"`
	Gzip     *bool `yaml:"gzip,omitempty" json:"gzip,omitempty"`
}

func NewS3DataPolicy() *S3DataPolicy {
	return &S3DataPolicy{
		Enabled:        true,
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
		MaxItems: DefaultDynamoDBMaxItems,
		MaxPages: DefaultDynamoDBMaxPages,
		Gzip:     &gzipEnabled,
	}
}

var accountID = regexp.MustCompile(`^[0-9]{12}$`)

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
		},
		Output: Output{Directory: ".floceed"},
	}
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
	if p.Output.Directory == "" {
		p.Output.Directory = ".floceed"
	}
	for i := range p.Resources.S3 {
		if p.Resources.S3[i].Data != nil && p.Resources.S3[i].Data.Overwrite == "" {
			p.Resources.S3[i].Data.Overwrite = OverwriteIfDifferent
		}
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
			if d.MaxObjects <= 0 || d.MaxObjectBytes <= 0 || d.MaxTotalBytes <= 0 {
				return fmt.Errorf("S3 %q data requires positive object and byte limits: %w", r.Name, ErrValidation)
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
		if d := r.Data; d != nil && d.Enabled && (d.MaxItems <= 0 || d.MaxPages <= 0) {
			return fmt.Errorf("DynamoDB %q data requires positive item and page limits: %w", r.Name, ErrValidation)
		}
	}
	return nil
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
