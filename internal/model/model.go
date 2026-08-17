package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/governance"
)

const CurrentManifestSchemaVersion = 3
const MinimumManifestSchemaVersion = 1
const CurrentSnapshotStructureVersion = 1

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

type SupportState string

const (
	SupportFull                SupportState = "full"
	SupportStructureOnly       SupportState = "structure_only"
	SupportPartial             SupportState = "partial"
	SupportImporterUnsupported SupportState = "importer_unsupported"
	SupportTargetUnsupported   SupportState = "target_unsupported"
)

type Finding struct {
	Code        string       `json:"code"`
	Severity    Severity     `json:"severity"`
	Support     SupportState `json:"support"`
	Resource    string       `json:"resource,omitempty"`
	Property    string       `json:"property,omitempty"`
	Message     string       `json:"message,omitempty"`
	Remediation string       `json:"remediation,omitempty"`
}

type ServiceDescriptor struct {
	Name        string       `json:"name"`
	DisplayName string       `json:"display_name"`
	Support     SupportState `json:"support"`
}
type SourceScope struct {
	Profile   string `json:"-"`
	AccountID string `json:"account_id,omitempty"`
	Region    string `json:"region"`
}
type ResourceRef struct {
	Service string `json:"service"`
	Type    string `json:"type"`
	ID      string `json:"id"`
	ARN     string `json:"arn,omitempty"`
}
type ResourceSummary struct {
	Ref        ResourceRef    `json:"ref"`
	Name       string         `json:"name"`
	Region     string         `json:"region,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Findings   []Finding      `json:"findings,omitempty"`
}
type DiscoveryResult struct {
	Resources []ResourceSummary `json:"resources"`
	Findings  []Finding         `json:"findings,omitempty"`
}
type CaptureOptions struct {
	IncludeData         bool                        `json:"include_data"`
	AllowPartialData    bool                        `json:"allow_partial_data,omitempty"`
	PreserveProvisioned bool                        `json:"preserve_provisioned,omitempty"`
	Gzip                bool                        `json:"gzip,omitempty"`
	Limits              DataLimits                  `json:"limits,omitempty"`
	Prefixes            []string                    `json:"prefixes,omitempty"`
	Overwrite           string                      `json:"overwrite,omitempty"`
	Mode                string                      `json:"mode,omitempty"`
	EstimatedRecords    int64                       `json:"estimated_records,omitempty"`
	EstimatedBytes      int64                       `json:"estimated_bytes,omitempty"`
	ArtifactDirectory   string                      `json:"-"`
	CheckpointDirectory string                      `json:"-"`
	Progress            func(ProgressEvent)         `json:"-"`
	Governance          *governance.EffectivePolicy `json:"-"`
	GovernanceAudit     *governance.Audit           `json:"-"`
}
type DataLimits struct {
	MaxObjects     int   `json:"max_objects,omitempty"`
	MaxItems       int   `json:"max_items,omitempty"`
	MaxPages       int   `json:"max_pages,omitempty"`
	MaxObjectBytes int64 `json:"max_object_bytes,omitempty"`
	MaxTotalBytes  int64 `json:"max_total_bytes,omitempty"`
}
type Dependency struct {
	From     ResourceRef `json:"from"`
	To       ResourceRef `json:"to"`
	Kind     string      `json:"kind"`
	Required bool        `json:"required"`
}
type Snapshot struct {
	Resource         ResourceRef     `json:"resource"`
	Service          string          `json:"service"`
	StructureVersion int             `json:"structure_version"`
	Structure        json.RawMessage `json:"structure"`
	Data             []ArtifactRef   `json:"data,omitempty"`
	Dataset          *Dataset        `json:"dataset,omitempty"`
	Findings         []Finding       `json:"findings,omitempty"`
}

func NewSnapshot(resource ResourceRef, service string, structure any) (*Snapshot, error) {
	payload, err := json.Marshal(structure)
	if err != nil {
		return nil, fmt.Errorf("encode %s snapshot structure: %w", service, err)
	}
	return &Snapshot{Resource: resource, Service: service, StructureVersion: CurrentSnapshotStructureVersion, Structure: payload}, nil
}

func DecodeStructure[T any](snapshot *Snapshot) (T, error) {
	var value T
	if snapshot == nil {
		return value, fmt.Errorf("decode snapshot structure: snapshot is nil: %w", ErrValidation)
	}
	if err := json.Unmarshal(snapshot.Structure, &value); err != nil {
		return value, fmt.Errorf("decode %s snapshot structure: %w", snapshot.Service, err)
	}
	return value, nil
}

func SetStructure(snapshot *Snapshot, structure any) error {
	payload, err := json.Marshal(structure)
	if err != nil {
		return fmt.Errorf("encode %s snapshot structure: %w", snapshot.Service, err)
	}
	snapshot.Structure = payload
	return nil
}

type ArtifactRef struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type,omitempty"`
}

type Dataset struct {
	Format      string      `json:"format"`
	Records     int64       `json:"records"`
	SourceBytes int64       `json:"source_bytes"`
	Consistency string      `json:"consistency"`
	Resumed     bool        `json:"resumed,omitempty"`
	Chunks      []DataChunk `json:"chunks"`
}

type DataChunk struct {
	Data        ArtifactRef  `json:"data"`
	Index       *ArtifactRef `json:"index,omitempty"`
	Records     int64        `json:"records"`
	SourceBytes int64        `json:"source_bytes"`
}

type ProgressEvent struct {
	SchemaVersion    int    `json:"schema_version"`
	Event            string `json:"event"`
	Operation        string `json:"operation"`
	Phase            string `json:"phase"`
	Service          string `json:"service,omitempty"`
	Resource         string `json:"resource,omitempty"`
	CompletedRecords int64  `json:"completed_records,omitempty"`
	TotalRecords     int64  `json:"total_records,omitempty"`
	CompletedBytes   int64  `json:"completed_bytes,omitempty"`
	TotalBytes       int64  `json:"total_bytes,omitempty"`
	CompletedChunks  int64  `json:"completed_chunks,omitempty"`
	TotalChunks      int64  `json:"total_chunks,omitempty"`
	TotalPrecision   string `json:"total_precision,omitempty"`
	Resumed          bool   `json:"resumed,omitempty"`
	Sequence         int64  `json:"sequence,omitempty"`
	Message          string `json:"message,omitempty"`
}
type Capabilities struct {
	FlociVersion string         `json:"floci_version"`
	Services     map[string]any `json:"services,omitempty"`
}

type Stage string

const (
	StageBase    Stage = "base"
	StageMutable Stage = "mutable"
	StageLinks   Stage = "links"
	StageData    Stage = "data"
)

type Operation struct {
	ID         string   `json:"id"`
	Stage      Stage    `json:"stage"`
	Service    string   `json:"service"`
	ResourceID string   `json:"resource_id"`
	Action     string   `json:"action"`
	DependsOn  []string `json:"depends_on,omitempty"`
}

type CaptureMetadata struct {
	CapturedAt time.Time `json:"captured_at"`
	Partial    bool      `json:"partial,omitempty"`
}
type ToolMetadata struct {
	Version string `json:"version"`
}
type TargetMetadata struct {
	FlociVersion string `json:"floci_version"`
	Image        string `json:"image"`
}
type SourceMetadata struct {
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

// Provenance is self-asserted metadata carried by an offline fixture. It is
// deliberately separate from Manifest: checksum verification proves only
// that these values are internally consistent, not that they are authentic.
type Provenance struct {
	SchemaVersion  int       `json:"schema_version"`
	AccountID      string    `json:"account_id"`
	Region         string    `json:"region"`
	CapturedAt     time.Time `json:"captured_at"`
	ManifestSchema int       `json:"manifest_schema"`
}

const CurrentProvenanceSchemaVersion = 1

type ProvenanceStatus string

const (
	ProvenanceSelfAsserted ProvenanceStatus = "self_asserted"
)

// VerificationResult is the stable, non-authenticating result of checking a
// fixture's bytes and schema.
type VerificationResult struct {
	Identity         string           `json:"identity"`
	ManifestSchema   int              `json:"manifest_schema"`
	FileCount        int              `json:"file_count"`
	TotalBytes       int64            `json:"total_bytes"`
	Provenance       *Provenance      `json:"provenance,omitempty"`
	ProvenanceStatus ProvenanceStatus `json:"provenance_status,omitempty"`
}

type CountBucket = governance.CountBucket

const (
	CountBucketZero       = governance.BucketZero
	CountBucket1To9       = governance.BucketOneToNine
	CountBucket10To99     = governance.BucketTenToNinetyNine
	CountBucket100To999   = governance.BucketHundredToNineHundredNinetyNine
	CountBucket1000OrMore = governance.BucketThousandPlus
)

type GovernanceRuleAudit struct {
	RuleID string      `json:"rule_id"`
	Action string      `json:"action"`
	Count  CountBucket `json:"count"`
}

type GovernanceCohortAudit struct {
	ResourceIdentity string      `json:"resource_identity"`
	Eligible         CountBucket `json:"eligible"`
	Retained         CountBucket `json:"retained"`
	Truncated        bool        `json:"truncated,omitempty"`
}

type GovernanceAudit struct {
	Profile        string                  `json:"profile"`
	PolicyIdentity string                  `json:"policy_identity"`
	CohortIdentity string                  `json:"cohort_identity,omitempty"`
	KeyIDs         []string                `json:"key_ids,omitempty"`
	Algorithms     []string                `json:"algorithms,omitempty"`
	Rules          []GovernanceRuleAudit   `json:"rules,omitempty"`
	Cohorts        []GovernanceCohortAudit `json:"cohorts,omitempty"`
}

type Manifest struct {
	SchemaVersion int              `json:"schema_version"`
	Tool          ToolMetadata     `json:"tool"`
	Target        TargetMetadata   `json:"target"`
	Source        SourceMetadata   `json:"source"`
	Capture       CaptureMetadata  `json:"capture"`
	Selected      []ResourceRef    `json:"selected"`
	Snapshots     []Snapshot       `json:"snapshots"`
	Operations    []Operation      `json:"operations"`
	Findings      []Finding        `json:"findings,omitempty"`
	Governance    *GovernanceAudit `json:"governance,omitempty"`
}

var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)

func (m Manifest) Validate() error {
	if m.SchemaVersion < MinimumManifestSchemaVersion || m.SchemaVersion > CurrentManifestSchemaVersion {
		return fmt.Errorf("unsupported manifest schema %d (runtime supports %d-%d): %w", m.SchemaVersion, MinimumManifestSchemaVersion, CurrentManifestSchemaVersion, ErrSchema)
	}
	if m.Source.AccountID != "" && !accountPattern.MatchString(m.Source.AccountID) {
		return fmt.Errorf("source account ID must be 12 digits: %w", ErrValidation)
	}
	selected := make(map[struct{ service, resourceType, id string }]struct{}, len(m.Selected))
	for index, resource := range m.Selected {
		identity := struct{ service, resourceType, id string }{resource.Service, resource.Type, resource.ID}
		if _, exists := selected[identity]; exists {
			return fmt.Errorf("selected resource %d duplicates an earlier identity: %w", index, ErrValidation)
		}
		selected[identity] = struct{}{}
	}
	for index := range m.Snapshots {
		if err := validateSnapshot(m.Snapshots[index]); err != nil {
			return fmt.Errorf("snapshot %d: %w", index, err)
		}
	}
	if m.SchemaVersion >= 2 {
		for index := range m.Snapshots {
			s := &m.Snapshots[index]
			if len(s.Data) != 0 {
				return fmt.Errorf("snapshot %d uses legacy data in manifest schema 2: %w", index, ErrValidation)
			}
			if s.Dataset != nil {
				if s.Dataset.Format == "" || s.Dataset.Records < 0 || s.Dataset.SourceBytes < 0 {
					return fmt.Errorf("snapshot %d has invalid dataset: %w", index, ErrValidation)
				}
				validFormat := (s.Service == "s3" && s.Dataset.Format == "s3-tar-gzip-v1") || (s.Service == "dynamodb" && (s.Dataset.Format == "dynamodb-ndjson-v1" || s.Dataset.Format == "dynamodb-ndjson-gzip-v1"))
				if !validFormat {
					return fmt.Errorf("snapshot %d has unsupported dataset format %q: %w", index, s.Dataset.Format, ErrValidation)
				}
				var records, sourceBytes int64
				for _, chunk := range s.Dataset.Chunks {
					if chunk.Data.Path == "" || chunk.Records < 0 || chunk.SourceBytes < 0 {
						return fmt.Errorf("snapshot %d has invalid dataset chunk: %w", index, ErrValidation)
					}
					if s.Service == "s3" && chunk.Index == nil {
						return fmt.Errorf("snapshot %d S3 dataset chunk requires an index: %w", index, ErrValidation)
					}
					records += chunk.Records
					sourceBytes += chunk.SourceBytes
				}
				if records != s.Dataset.Records || sourceBytes != s.Dataset.SourceBytes {
					return fmt.Errorf("snapshot %d dataset totals do not match chunks: %w", index, ErrValidation)
				}
			}
		}
	}
	if m.Governance != nil {
		if m.SchemaVersion < 3 {
			return fmt.Errorf("governance requires manifest schema 3: %w", ErrValidation)
		}
		if strings.TrimSpace(m.Governance.Profile) == "" || strings.TrimSpace(m.Governance.PolicyIdentity) == "" {
			return fmt.Errorf("governance profile and policy identity are required: %w", ErrValidation)
		}
		if !uniqueNonEmpty(m.Governance.KeyIDs) || !uniqueNonEmpty(m.Governance.Algorithms) {
			return fmt.Errorf("governance identities must be non-empty and unique: %w", ErrValidation)
		}
		ruleIDs := make(map[string]bool, len(m.Governance.Rules))
		for _, rule := range m.Governance.Rules {
			validAction := rule.Action == string(governance.ActionOmit) || rule.Action == string(governance.ActionReplace) || rule.Action == string(governance.ActionHash) || rule.Action == string(governance.ActionPseudonymize)
			if strings.TrimSpace(rule.RuleID) == "" || ruleIDs[rule.RuleID] || !validAction || !validCountBucket(rule.Count) {
				return fmt.Errorf("governance rule audit is invalid: %w", ErrValidation)
			}
			ruleIDs[rule.RuleID] = true
		}
		cohortIDs := make(map[string]bool, len(m.Governance.Cohorts))
		for _, cohort := range m.Governance.Cohorts {
			if strings.TrimSpace(cohort.ResourceIdentity) == "" || cohortIDs[cohort.ResourceIdentity] || !validCountBucket(cohort.Eligible) || !validCountBucket(cohort.Retained) {
				return fmt.Errorf("governance cohort audit is invalid: %w", ErrValidation)
			}
			cohortIDs[cohort.ResourceIdentity] = true
		}
	}
	return nil
}

func uniqueNonEmpty(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func validCountBucket(bucket CountBucket) bool {
	return bucket == CountBucketZero || bucket == CountBucket1To9 || bucket == CountBucket10To99 || bucket == CountBucket100To999 || bucket == CountBucket1000OrMore
}

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.Service == "" || snapshot.Service != snapshot.Resource.Service {
		return fmt.Errorf("service must match resource service: %w", ErrValidation)
	}
	if snapshot.StructureVersion != CurrentSnapshotStructureVersion {
		return fmt.Errorf("unsupported %s structure version %d (runtime supports %d): %w", snapshot.Service, snapshot.StructureVersion, CurrentSnapshotStructureVersion, ErrSchema)
	}
	if len(bytes.TrimSpace(snapshot.Structure)) == 0 || bytes.Equal(bytes.TrimSpace(snapshot.Structure), []byte("null")) {
		return fmt.Errorf("%s structure is required: %w", snapshot.Service, ErrValidation)
	}
	var required struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(snapshot.Structure, &required); err != nil {
		return fmt.Errorf("invalid %s structure: %w", snapshot.Service, ErrValidation)
	}
	if required.Name == "" {
		return fmt.Errorf("%s structure name is required: %w", snapshot.Service, ErrValidation)
	}
	if snapshot.Resource.ID == "" || required.Name != snapshot.Resource.ID {
		return fmt.Errorf("%s structure name must match resource ID: %w", snapshot.Service, ErrValidation)
	}
	switch snapshot.Service {
	case "s3":
		var value struct {
			Region string `json:"region"`
		}
		if err := json.Unmarshal(snapshot.Structure, &value); err != nil || value.Region == "" {
			return fmt.Errorf("S3 structure region is required: %w", ErrValidation)
		}
	case "dynamodb":
		var value struct {
			Attributes  []struct{} `json:"attribute_definitions"`
			Keys        []struct{} `json:"key_schema"`
			BillingMode string     `json:"billing_mode"`
		}
		if err := json.Unmarshal(snapshot.Structure, &value); err != nil || value.Attributes == nil || len(value.Keys) == 0 || value.BillingMode == "" {
			return fmt.Errorf("DynamoDB structure requires attribute_definitions, key_schema, and billing_mode: %w", ErrValidation)
		}
	case "sqs", "sns":
		var value struct {
			ARN string `json:"arn"`
		}
		if err := json.Unmarshal(snapshot.Structure, &value); err != nil || value.ARN == "" {
			return fmt.Errorf("%s structure requires arn: %w", snapshot.Service, ErrValidation)
		}
		parts := strings.Split(value.ARN, ":")
		if len(parts) != 6 || parts[0] != "arn" || parts[1] == "" || parts[2] != snapshot.Service || parts[3] == "" || !snapshotAccountID.MatchString(parts[4]) || parts[5] != snapshot.Resource.ID {
			return fmt.Errorf("%s structure ARN must match resource identity: %w", snapshot.Service, ErrValidation)
		}
		if snapshot.Resource.ARN != "" && snapshot.Resource.ARN != value.ARN {
			return fmt.Errorf("%s structure ARN must match resource ARN: %w", snapshot.Service, ErrValidation)
		}
	default:
		return fmt.Errorf("unsupported snapshot service %q: %w", snapshot.Service, ErrValidation)
	}
	return nil
}

var ErrSchema = errors.New("schema version error")
var ErrValidation = errors.New("validation error")
var snapshotAccountID = regexp.MustCompile(`^[0-9]{12}$`)
