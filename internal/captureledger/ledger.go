package captureledger

import (
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
)

const CurrentSchemaVersion = 1

type UnitOutcome string

const (
	UnitOutcomeReused      UnitOutcome = "reused"
	UnitOutcomeRefreshed   UnitOutcome = "refreshed"
	UnitOutcomeInvalidated UnitOutcome = "invalidated"
)

type Reason string

const (
	ReasonNoCandidate              Reason = "no_candidate"
	ReasonCaptureDefinitionChanged Reason = "capture_definition_changed"
	ReasonFormatChanged            Reason = "format_changed"
	ReasonSourceContentChanged     Reason = "source_content_changed"
	ReasonSourceUnitMissing        Reason = "source_unit_missing"
	ReasonFreshnessUnproven        Reason = "freshness_unproven"
	ReasonArtifactMissing          Reason = "artifact_missing"
	ReasonArtifactCorrupt          Reason = "artifact_corrupt"
	ReasonReused                   Reason = "reused"
)

func (reason Reason) Valid() bool {
	switch reason {
	case ReasonNoCandidate, ReasonCaptureDefinitionChanged, ReasonFormatChanged, ReasonSourceContentChanged, ReasonSourceUnitMissing, ReasonFreshnessUnproven, ReasonArtifactMissing, ReasonArtifactCorrupt, ReasonReused:
		return true
	default:
		return false
	}
}

type Generation struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"id"`
	Source        SourceIdentity `json:"source"`
	CreatedAt     time.Time      `json:"created_at"`
	CompletedAt   time.Time      `json:"completed_at"`
	Resources     []Resource     `json:"resources"`
}

type Resource struct {
	Descriptor        ResourceDescriptor `json:"descriptor"`
	CaptureDefinition string             `json:"capture_definition"`
	Units             []Unit             `json:"units"`
}

type Unit struct {
	ID         string            `json:"id"`
	Freshness  FreshnessEvidence `json:"freshness"`
	Artifacts  []Artifact        `json:"artifacts,omitempty"`
	Outcome    UnitOutcome       `json:"outcome"`
	Reason     Reason            `json:"reason"`
	CapturedAt time.Time         `json:"captured_at"`
}

// FreshnessEvidence contains a service-defined evidence class and only its
// digest. Raw object keys, ETags, records, and payload values do not belong in
// ledger metadata.
type FreshnessEvidence struct {
	Kind       string            `json:"kind"`
	Digest     string            `json:"digest"`
	Components map[string]string `json:"components,omitempty"`
}

type Artifact struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type,omitempty"`
}

func (generation Generation) Validate() error {
	if generation.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("unsupported ledger schema %d", generation.SchemaVersion)
	}
	if !validSHA256(generation.ID) {
		return fmt.Errorf("generation ID must be a SHA-256 digest")
	}
	if err := generation.Source.validate(); err != nil {
		return err
	}
	if generation.CreatedAt.IsZero() || generation.CompletedAt.IsZero() || generation.CompletedAt.Before(generation.CreatedAt) {
		return fmt.Errorf("valid generation timestamps are required")
	}
	resources := make(map[string]struct{}, len(generation.Resources))
	artifacts := make(map[string]struct{})
	for resourceIndex, resource := range generation.Resources {
		if err := resource.Descriptor.validate(); err != nil {
			return fmt.Errorf("resource %d: %w", resourceIndex, err)
		}
		resourceKey := descriptorKey(resource.Descriptor)
		if _, exists := resources[resourceKey]; exists {
			return fmt.Errorf("duplicate resource identity %q", resourceKey)
		}
		resources[resourceKey] = struct{}{}
		if !validSHA256(resource.CaptureDefinition) {
			return fmt.Errorf("resource %d capture definition must be a SHA-256 digest", resourceIndex)
		}
		units := make(map[string]struct{}, len(resource.Units))
		for unitIndex, unit := range resource.Units {
			if strings.TrimSpace(unit.ID) == "" {
				return fmt.Errorf("resource %d unit %d ID is required", resourceIndex, unitIndex)
			}
			if _, exists := units[unit.ID]; exists {
				return fmt.Errorf("resource %d has duplicate unit ID %q", resourceIndex, unit.ID)
			}
			units[unit.ID] = struct{}{}
			if err := unit.validate(); err != nil {
				return fmt.Errorf("resource %d unit %q: %w", resourceIndex, unit.ID, err)
			}
			for _, artifact := range unit.Artifacts {
				if _, exists := artifacts[artifact.Path]; exists {
					return fmt.Errorf("duplicate artifact path %q", artifact.Path)
				}
				artifacts[artifact.Path] = struct{}{}
			}
		}
	}
	return nil
}

func (unit Unit) validate() error {
	if strings.TrimSpace(unit.Freshness.Kind) == "" || !validSHA256(unit.Freshness.Digest) {
		return fmt.Errorf("freshness kind and SHA-256 digest are required")
	}
	for name, digest := range unit.Freshness.Components {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "\r\n\t") || !validSHA256(digest) {
			return fmt.Errorf("freshness components must have safe names and SHA-256 digests")
		}
	}
	if unit.Outcome != UnitOutcomeReused && unit.Outcome != UnitOutcomeRefreshed && unit.Outcome != UnitOutcomeInvalidated {
		return fmt.Errorf("unsupported outcome %q", unit.Outcome)
	}
	if !unit.Reason.Valid() {
		return fmt.Errorf("unsupported reason %q", unit.Reason)
	}
	if (unit.Outcome == UnitOutcomeReused) != (unit.Reason == ReasonReused) {
		return fmt.Errorf("reused outcome and reason must agree")
	}
	if unit.CapturedAt.IsZero() {
		return fmt.Errorf("captured timestamp is required")
	}
	for index, artifact := range unit.Artifacts {
		if !safeRelativePath(artifact.Path) {
			return fmt.Errorf("artifact %d has unsafe path %q", index, artifact.Path)
		}
		if artifact.Size < 0 || !validSHA256(artifact.SHA256) {
			return fmt.Errorf("artifact %d has invalid size or SHA-256", index)
		}
	}
	return nil
}

func safeRelativePath(value string) bool {
	return value != "" && !strings.Contains(value, `\`) && !strings.HasPrefix(value, "/") && path.Clean(value) == value && value != "." && value != ".." && !strings.HasPrefix(value, "../")
}

func descriptorKey(descriptor ResourceDescriptor) string {
	return descriptor.Service + "\x00" + descriptor.Type + "\x00" + descriptor.ID
}

// CanonicalJSON validates and sorts a copy, leaving caller-owned slices
// untouched. Its output is suitable for atomic generation storage and hashing.
func (generation Generation) CanonicalJSON() ([]byte, error) {
	if err := generation.Validate(); err != nil {
		return nil, err
	}
	canonical := generation
	canonical.CreatedAt = canonical.CreatedAt.UTC()
	canonical.CompletedAt = canonical.CompletedAt.UTC()
	canonical.Resources = append([]Resource(nil), generation.Resources...)
	for index := range canonical.Resources {
		canonical.Resources[index].Units = append([]Unit(nil), canonical.Resources[index].Units...)
		for unitIndex := range canonical.Resources[index].Units {
			canonical.Resources[index].Units[unitIndex].CapturedAt = canonical.Resources[index].Units[unitIndex].CapturedAt.UTC()
			canonical.Resources[index].Units[unitIndex].Artifacts = append([]Artifact(nil), canonical.Resources[index].Units[unitIndex].Artifacts...)
			slices.SortFunc(canonical.Resources[index].Units[unitIndex].Artifacts, func(a, b Artifact) int { return strings.Compare(a.Path, b.Path) })
		}
		slices.SortFunc(canonical.Resources[index].Units, func(a, b Unit) int { return strings.Compare(a.ID, b.ID) })
	}
	slices.SortFunc(canonical.Resources, func(a, b Resource) int {
		return strings.Compare(descriptorKey(a.Descriptor), descriptorKey(b.Descriptor))
	})
	payload, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("encode ledger generation: %w", err)
	}
	return payload, nil
}
