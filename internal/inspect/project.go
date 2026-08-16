package inspect

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/model"
)

// ProjectManifest constructs an allowlisted, deterministic semantic view.
func ProjectManifest(manifest model.Manifest) (Projection, error) {
	if err := manifest.Validate(); err != nil {
		return Projection{}, fmt.Errorf("project manifest: %w", err)
	}
	p := Projection{
		SchemaVersion: ProjectionSchemaVersion,
		Source:        SourceProjection{AccountID: manifest.Source.AccountID, Region: manifest.Source.Region},
		Target:        TargetProjection{FlociVersion: manifest.Target.FlociVersion, Image: manifest.Target.Image},
		Governance:    projectGovernance(manifest.Governance),
	}
	selected := make(map[string]bool, len(manifest.Selected))
	refs := make(map[string]model.ResourceRef, len(manifest.Selected)+len(manifest.Snapshots))
	for _, ref := range manifest.Selected {
		key := resourceKey(ref)
		selected[key], refs[key] = true, ref
	}
	structures := make(map[string]string, len(manifest.Snapshots))
	datasets := make(map[string]string, len(manifest.Snapshots))
	snapshotFindings := make(map[string][]Finding, len(manifest.Snapshots))
	for _, snapshot := range manifest.Snapshots {
		key := resourceKey(snapshot.Resource)
		if _, exists := structures[key]; exists {
			return Projection{}, fmt.Errorf("project manifest: duplicate snapshot %s", key)
		}
		refs[key] = snapshot.Resource
		structure, err := canonicalStructure(snapshot.Structure)
		if err != nil {
			return Projection{}, fmt.Errorf("project %s structure: %w", key, err)
		}
		structures[key] = digestBytes(structure)
		snapshotFindings[key] = ProjectFindings(snapshot.Findings)
		dataset, err := projectDataset(snapshot)
		if err != nil {
			return Projection{}, fmt.Errorf("project %s dataset: %w", key, err)
		}
		if dataset != nil {
			datasets[key], err = digestValue(dataset)
			if err != nil {
				return Projection{}, err
			}
		}
	}
	for _, operation := range manifest.Operations {
		op := ProjectedOperation{ID: operation.ID, Stage: string(operation.Stage), Service: operation.Service, ResourceID: operation.ResourceID, Action: operation.Action, DependsOn: append([]string(nil), operation.DependsOn...)}
		sort.Strings(op.DependsOn)
		p.Operations = append(p.Operations, op)
	}
	sort.Slice(p.Operations, func(i, j int) bool { return operationKey(p.Operations[i]) < operationKey(p.Operations[j]) })
	p.Findings = ProjectFindings(manifest.Findings)
	operationsByResource := make(map[string][]ProjectedOperation)
	for _, operation := range p.Operations {
		key := operation.Service + "\x00" + operation.ResourceID
		operationsByResource[key] = append(operationsByResource[key], operation)
	}
	findingsByResource := make(map[string][]Finding)
	for _, finding := range p.Findings {
		findingsByResource[finding.Resource] = append(findingsByResource[finding.Resource], finding)
	}
	governanceDigest, err := optionalDigest(p.Governance)
	if err != nil {
		return Projection{}, err
	}
	keys := make([]string, 0, len(refs))
	for key := range refs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ref := refs[key]
		resourceOps := operationsByResource[ref.Service+"\x00"+ref.ID]
		resourceFindings := append([]Finding(nil), findingsByResource[ref.ID]...)
		resourceFindings = append(resourceFindings, snapshotFindings[key]...)
		sort.Slice(resourceFindings, func(i, j int) bool { return findingKey(resourceFindings[i]) < findingKey(resourceFindings[j]) })
		operationsDigest, err := optionalDigest(resourceOps)
		if err != nil {
			return Projection{}, err
		}
		findingsDigest, err := optionalDigest(resourceFindings)
		if err != nil {
			return Projection{}, err
		}
		p.Resources = append(p.Resources, ProjectedResource{
			Identity: identity(ref), Selected: selected[key], StructureDigest: structures[key], DatasetDigest: datasets[key],
			GovernanceDigest: governanceDigest, OperationsDigest: operationsDigest, FindingsDigest: findingsDigest,
		})
	}
	canonical, err := bundle.CanonicalJSON(struct {
		SchemaVersion int                  `json:"schema_version"`
		Source        SourceProjection     `json:"source"`
		Target        TargetProjection     `json:"target"`
		Resources     []ProjectedResource  `json:"resources"`
		Operations    []ProjectedOperation `json:"operations,omitempty"`
		Findings      []Finding            `json:"findings,omitempty"`
		Governance    *GovernanceSummary   `json:"governance,omitempty"`
	}{p.SchemaVersion, p.Source, p.Target, p.Resources, p.Operations, p.Findings, p.Governance})
	if err != nil {
		return Projection{}, err
	}
	p.Digest = digestBytes(canonical)
	return p, nil
}

type projectedDataset struct {
	Format      string              `json:"format,omitempty"`
	Records     int64               `json:"records"`
	SourceBytes int64               `json:"source_bytes"`
	Consistency string              `json:"consistency,omitempty"`
	Chunks      []projectedChunk    `json:"chunks,omitempty"`
	Legacy      []projectedArtifact `json:"legacy,omitempty"`
}
type projectedChunk struct {
	Data        projectedArtifact  `json:"data"`
	Index       *projectedArtifact `json:"index,omitempty"`
	Records     int64              `json:"records"`
	SourceBytes int64              `json:"source_bytes"`
}
type projectedArtifact struct {
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type,omitempty"`
}

func projectDataset(s model.Snapshot) (*projectedDataset, error) {
	if s.Dataset == nil && len(s.Data) == 0 {
		return nil, nil
	}
	d := &projectedDataset{}
	if s.Dataset != nil {
		d.Format, d.Records, d.SourceBytes, d.Consistency = s.Dataset.Format, s.Dataset.Records, s.Dataset.SourceBytes, s.Dataset.Consistency
		for _, chunk := range s.Dataset.Chunks {
			c := projectedChunk{Data: projectArtifact(chunk.Data), Records: chunk.Records, SourceBytes: chunk.SourceBytes}
			if chunk.Index != nil {
				value := projectArtifact(*chunk.Index)
				c.Index = &value
			}
			d.Chunks = append(d.Chunks, c)
		}
		sort.Slice(d.Chunks, func(i, j int) bool { return chunkKey(d.Chunks[i]) < chunkKey(d.Chunks[j]) })
	} else {
		for _, artifact := range s.Data {
			d.Legacy = append(d.Legacy, projectArtifact(artifact))
		}
		sort.Slice(d.Legacy, func(i, j int) bool { return artifactKey(d.Legacy[i]) < artifactKey(d.Legacy[j]) })
	}
	return d, nil
}

func projectArtifact(a model.ArtifactRef) projectedArtifact {
	return projectedArtifact{SHA256: a.SHA256, Size: a.Size, MediaType: a.MediaType}
}
func artifactKey(a projectedArtifact) string {
	return a.SHA256 + "\x00" + fmt.Sprint(a.Size) + "\x00" + a.MediaType
}
func chunkKey(c projectedChunk) string {
	index := ""
	if c.Index != nil {
		index = artifactKey(*c.Index)
	}
	return artifactKey(c.Data) + "\x00" + index + "\x00" + fmt.Sprint(c.Records) + "\x00" + fmt.Sprint(c.SourceBytes)
}

func canonicalStructure(raw json.RawMessage) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return bundle.CanonicalJSON(value)
}

func projectGovernance(a *model.GovernanceAudit) *GovernanceSummary {
	if a == nil {
		return nil
	}
	g := &GovernanceSummary{Profile: a.Profile, PolicyIdentity: a.PolicyIdentity, CohortIdentity: a.CohortIdentity, KeyIDs: append([]string(nil), a.KeyIDs...), Algorithms: append([]string(nil), a.Algorithms...)}
	sort.Strings(g.KeyIDs)
	sort.Strings(g.Algorithms)
	for _, rule := range a.Rules {
		g.Rules = append(g.Rules, GovernanceRule{ID: rule.RuleID, Action: rule.Action, Count: string(rule.Count)})
	}
	sort.Slice(g.Rules, func(i, j int) bool {
		return g.Rules[i].ID+"\x00"+g.Rules[i].Action < g.Rules[j].ID+"\x00"+g.Rules[j].Action
	})
	for _, cohort := range a.Cohorts {
		g.Cohorts = append(g.Cohorts, GovernanceCohort{ResourceIdentity: cohort.ResourceIdentity, Eligible: string(cohort.Eligible), Retained: string(cohort.Retained), Truncated: cohort.Truncated})
	}
	sort.Slice(g.Cohorts, func(i, j int) bool { return g.Cohorts[i].ResourceIdentity < g.Cohorts[j].ResourceIdentity })
	return g
}

// ProjectFindings returns the disclosure-safe canonical finding projection.
func ProjectFindings(in []model.Finding) []Finding {
	out := make([]Finding, 0, len(in))
	for _, f := range in {
		out = append(out, Finding{Code: f.Code, Severity: string(f.Severity), Support: string(f.Support), Resource: f.Resource, Property: f.Property})
	}
	sort.Slice(out, func(i, j int) bool { return findingKey(out[i]) < findingKey(out[j]) })
	return out
}
func findingKey(f Finding) string {
	return f.Code + "\x00" + f.Resource + "\x00" + f.Property + "\x00" + f.Severity + "\x00" + f.Support
}
func operationKey(o ProjectedOperation) string {
	return o.ID + "\x00" + o.Stage + "\x00" + o.Service + "\x00" + o.ResourceID + "\x00" + o.Action + "\x00" + strings.Join(o.DependsOn, "\x00")
}
func resourceKey(r model.ResourceRef) string { return r.Service + "\x00" + r.Type + "\x00" + r.ID }
func identity(r model.ResourceRef) ResourceIdentity {
	return ResourceIdentity{Service: r.Service, Type: r.Type, ID: r.ID}
}
func digestBytes(value []byte) string { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }
func digestValue(value any) (string, error) {
	b, err := bundle.CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return digestBytes(b), nil
}
func optionalDigest(value any) (string, error) {
	b, err := bundle.CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	trimmed := bytes.TrimSpace(b)
	if bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("[]")) {
		return "", nil
	}
	return digestBytes(b), nil
}
