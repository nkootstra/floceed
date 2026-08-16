// Package inspect defines the stable, disclosure-safe read model for generated
// bundles. The persisted manifest remains owned by package model.
package inspect

const (
	InspectionSchemaVersion = 1
	ReceiptSchemaVersion    = 1
	ProjectionSchemaVersion = 1
)

type Outcome string

const (
	OutcomeAdded     Outcome = "added"
	OutcomeRemoved   Outcome = "removed"
	OutcomeChanged   Outcome = "changed"
	OutcomeUnchanged Outcome = "unchanged"
)

type ChangeCategory string

const (
	CategoryStructure  ChangeCategory = "structure"
	CategoryDataset    ChangeCategory = "dataset"
	CategoryGovernance ChangeCategory = "governance"
	CategoryOperations ChangeCategory = "operations"
	CategoryFindings   ChangeCategory = "findings"
	CategorySelection  ChangeCategory = "selection"
	CategorySource     ChangeCategory = "source"
	CategoryTarget     ChangeCategory = "target"
)

type RuntimeState string

const (
	RuntimeNotRequested RuntimeState = "not_requested"
	RuntimeReady        RuntimeState = "ready"
	RuntimeNotReady     RuntimeState = "not_ready"
	RuntimeUnavailable  RuntimeState = "unavailable"
)

type ResourceIdentity struct {
	Service string `json:"service"`
	Type    string `json:"type"`
	ID      string `json:"id"`
}

type DatasetSummary struct {
	Format      string `json:"format"`
	Records     int64  `json:"records"`
	SourceBytes int64  `json:"source_bytes"`
	Chunks      int    `json:"chunks"`
	Resumed     bool   `json:"resumed,omitempty"`
}

type GovernanceSummary struct {
	Profile        string             `json:"profile"`
	PolicyIdentity string             `json:"policy_identity"`
	CohortIdentity string             `json:"cohort_identity,omitempty"`
	KeyIDs         []string           `json:"key_ids,omitempty"`
	Algorithms     []string           `json:"algorithms,omitempty"`
	Rules          []GovernanceRule   `json:"rules,omitempty"`
	Cohorts        []GovernanceCohort `json:"cohorts,omitempty"`
}

type GovernanceRule struct {
	ID     string `json:"id"`
	Action string `json:"action"`
	Count  string `json:"count"`
}

type GovernanceCohort struct {
	ResourceIdentity string `json:"resource_identity"`
	Eligible         string `json:"eligible"`
	Retained         string `json:"retained"`
	Truncated        bool   `json:"truncated,omitempty"`
}

type Resource struct {
	Identity   ResourceIdentity   `json:"identity"`
	Selected   bool               `json:"selected"`
	Dataset    *DatasetSummary    `json:"dataset,omitempty"`
	Governance *GovernanceSummary `json:"governance,omitempty"`
	Findings   []Finding          `json:"findings,omitempty"`
}

type Finding struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Support  string `json:"support"`
	Resource string `json:"resource,omitempty"`
	Property string `json:"property,omitempty"`
}

type ArtifactSummary struct {
	Files int   `json:"files"`
	Bytes int64 `json:"bytes"`
}

type ServiceSummary struct {
	Service     string `json:"service"`
	Resources   int    `json:"resources"`
	Selected    int    `json:"selected"`
	Records     int64  `json:"records"`
	SourceBytes int64  `json:"source_bytes"`
}

type Runtime struct {
	State         RuntimeState `json:"state"`
	FailedScripts []string     `json:"failed_scripts,omitempty"`
	Diagnostic    string       `json:"diagnostic,omitempty"`
}

type Inspection struct {
	SchemaVersion     int                  `json:"schema_version"`
	Valid             bool                 `json:"valid"`
	ManifestSchema    int                  `json:"manifest_schema"`
	BundleIdentity    string               `json:"bundle_identity"`
	ToolVersion       string               `json:"tool_version,omitempty"`
	CapturedAt        string               `json:"captured_at,omitempty"`
	Partial           bool                 `json:"partial,omitempty"`
	SelectedResources int                  `json:"selected_resources"`
	Source            SourceProjection     `json:"source"`
	Target            TargetProjection     `json:"target"`
	Artifacts         ArtifactSummary      `json:"artifacts"`
	Services          []ServiceSummary     `json:"services,omitempty"`
	Resources         []Resource           `json:"resources"`
	Governance        *GovernanceSummary   `json:"governance,omitempty"`
	Operations        []ProjectedOperation `json:"operations,omitempty"`
	Findings          []Finding            `json:"findings,omitempty"`
	Runtime           Runtime              `json:"runtime"`
	Receipt           *Receipt             `json:"receipt,omitempty"`
}

type ReceiptCounts struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Changed   int `json:"changed"`
	Unchanged int `json:"unchanged"`
}

type ResourceChange struct {
	Resource   ResourceIdentity `json:"resource"`
	Outcome    Outcome          `json:"outcome"`
	Categories []ChangeCategory `json:"categories,omitempty"`
}

type Receipt struct {
	SchemaVersion int              `json:"schema_version"`
	Baseline      string           `json:"baseline,omitempty"`
	Current       string           `json:"current"`
	Categories    []ChangeCategory `json:"categories,omitempty"`
	Counts        ReceiptCounts    `json:"counts"`
	Resources     []ResourceChange `json:"resources"`
}

type SourceProjection struct {
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

type TargetProjection struct {
	FlociVersion string `json:"floci_version"`
	Image        string `json:"image"`
}

// Projection is the canonical semantic view used for comparison. It contains
// identities and digests only, never raw resource structures or fixture data.
type Projection struct {
	SchemaVersion int                  `json:"schema_version"`
	Digest        string               `json:"digest"`
	Source        SourceProjection     `json:"source"`
	Target        TargetProjection     `json:"target"`
	Resources     []ProjectedResource  `json:"resources"`
	Operations    []ProjectedOperation `json:"operations,omitempty"`
	Findings      []Finding            `json:"findings,omitempty"`
	Governance    *GovernanceSummary   `json:"governance,omitempty"`
}

type ProjectedResource struct {
	Identity         ResourceIdentity `json:"identity"`
	Selected         bool             `json:"selected"`
	StructureDigest  string           `json:"structure_digest,omitempty"`
	DatasetDigest    string           `json:"dataset_digest,omitempty"`
	GovernanceDigest string           `json:"governance_digest,omitempty"`
	OperationsDigest string           `json:"operations_digest,omitempty"`
	FindingsDigest   string           `json:"findings_digest,omitempty"`
}

type ProjectedOperation struct {
	ID         string   `json:"id"`
	Stage      string   `json:"stage"`
	Service    string   `json:"service"`
	ResourceID string   `json:"resource_id"`
	Action     string   `json:"action"`
	DependsOn  []string `json:"depends_on,omitempty"`
}
