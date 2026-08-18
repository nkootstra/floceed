package app

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

const captureConcurrency = 4

type Plan struct {
	Source             model.SourceMetadata   `json:"source"`
	Selected           []model.ResourceRef    `json:"selected"`
	Operations         []model.Operation      `json:"operations"`
	Dependencies       []model.Dependency     `json:"dependencies,omitempty"`
	Findings           []model.Finding        `json:"findings,omitempty"`
	EstimatedBytes     int64                  `json:"estimated_bytes"`
	RequiredIAMActions []string               `json:"required_iam_actions"`
	Governance         *model.GovernanceAudit `json:"governance,omitempty"`
}

type captureResult struct {
	Plan              Plan
	Snapshots         []model.Snapshot
	LedgerResources   []captureledger.Resource
	LedgerGenerations map[string]string
}

func (a *Application) Plan(ctx context.Context, p config.Project, profile, region string) (Plan, error) {
	return a.PlanWithOptions(ctx, p, PlanOptions{AWSProfile: profile, Region: region})

}

type PlanOptions struct {
	AWSProfile     string
	Region         string
	FixtureProfile string
}

func (a *Application) PlanWithOptions(ctx context.Context, p config.Project, options PlanOptions) (Plan, error) {
	policy, err := resolveGovernance(p, options.FixtureProfile)
	if err != nil {
		return Plan{}, err
	}
	result, err := a.capture(ctx, captureRequest{Project: p, Profile: options.AWSProfile, Region: options.Region, Governance: policy})
	return result.Plan, err
}

func resolveGovernance(project config.Project, fixtureProfile string) (*governance.EffectivePolicy, error) {
	policy, err := project.ResolveFixtureProfile(fixtureProfile, os.Getenv)
	if err != nil {
		return nil, &Error{Kind: ErrorPlan, Code: "FIXTURE_PROFILE_INVALID", Message: err.Error(), Err: err}
	}
	return policy, nil
}

type captureRequest struct {
	Project        config.Project
	Profile        string
	Governance     *governance.EffectivePolicy
	Region         string
	ArtifactRoot   string
	IncludeData    bool
	CheckpointRoot string
	Progress       func(model.ProgressEvent)
	Source         *Source
	Ledger         *captureledger.Store
	LedgerSource   captureledger.SourceIdentity
}

func (a *Application) capture(ctx context.Context, req captureRequest) (captureResult, error) {
	p, profile, region := req.Project, req.Profile, req.Region
	policy := req.Governance
	if profile == "" {
		profile = p.Source.Profile
	}
	if region == "" {
		region = p.Source.Region
	}
	var source Source
	if req.Source != nil {
		source = *req.Source
	} else {
		var err error
		source, err = a.Factory.Open(ctx, SourceRequest{Profile: profile, Region: region, S3Names: s3Names(p), DynamoDBNames: ddbNames(p)})
		if err != nil {
			return captureResult{}, sourceError(err)
		}
	}
	if p.Source.ExpectedAccountID != "" && p.Source.ExpectedAccountID != source.Identity.AccountID {
		return captureResult{}, &Error{Kind: ErrorSource, Code: "SOURCE_ACCOUNT_MISMATCH", Message: fmt.Sprintf("AWS profile resolved to account %s, expected %s", source.Identity.AccountID, p.Source.ExpectedAccountID)}
	}
	result := Plan{Source: model.SourceMetadata{AccountID: source.Identity.AccountID, Region: region}, RequiredIAMActions: []string{"sts:GetCallerIdentity"}, Governance: governanceAuditForPolicy(policy)}
	var selections []catalog.Selection
	for _, adapter := range source.Registry.All() {
		contribution := adapter.Plan(p, req.IncludeData)
		selections = append(selections, contribution.Selections...)
		result.RequiredIAMActions = append(result.RequiredIAMActions, contribution.RequiredIAMActions...)
	}
	sort.Slice(selections, func(i, j int) bool {
		return cmp.Or(cmp.Compare(selections[i].Resource.Service, selections[j].Resource.Service), cmp.Compare(selections[i].Resource.ID, selections[j].Resource.ID)) < 0
	})
	type captureJob struct {
		adapter       catalog.Adapter
		options       model.CaptureOptions
		candidate     *captureledger.Resource
		invalidReason captureledger.Reason
		generationID  string
	}
	jobs := make([]captureJob, len(selections))
	for i, selection := range selections {
		adapter, ok := source.Registry.Get(selection.Resource.Service)
		if !ok {
			return captureResult{}, &Error{Kind: ErrorPlan, Code: "ADAPTER_MISSING", Message: "no adapter for " + selection.Resource.Service}
		}
		options := selection.Options
		options.Governance = policy
		audit := governance.NewAudit()
		options.GovernanceAudit = audit
		options.AllowPartialData = p.Capture.AllowPartialData
		if req.IncludeData {
			options.ArtifactDirectory = req.ArtifactRoot
			if req.CheckpointRoot != "" {
				sum := sha256.Sum256([]byte(selection.Resource.Service + "\x00" + selection.Resource.ID))
				options.CheckpointDirectory = filepath.Join(req.CheckpointRoot, hex.EncodeToString(sum[:16]))
			}
			options.Progress = req.Progress
		} else {
			options.IncludeData = false
		}
		job := captureJob{adapter: adapter, options: options}
		if req.IncludeData && req.Ledger != nil {
			descriptor := captureledger.ResourceDescriptor{Service: selection.Resource.Service, Type: selection.Resource.Type, ID: selection.Resource.ID}
			generation, loadErr := req.Ledger.LoadCandidates(req.LedgerSource, descriptor)
			if loadErr != nil {
				job.invalidReason, _ = captureledger.InvalidationReason(loadErr)
			} else {
				job.generationID = generation.ID
				for _, resource := range generation.Resources {
					if resource.Descriptor == descriptor {
						candidate := resource
						job.candidate = &candidate
						for _, unit := range resource.Units {
							if unit.Outcome == captureledger.UnitOutcomeInvalidated && job.invalidReason == "" {
								job.invalidReason = unit.Reason
							}
						}
					}
				}
			}
		}
		jobs[i] = job
	}

	type captureOutcome struct {
		snapshot *model.Snapshot
		resource *captureledger.Resource
		err      error
	}
	outcomes := make([]captureOutcome, len(jobs))
	captureCtx, cancelCaptures := context.WithCancel(ctx)
	defer cancelCaptures()
	concurrency := p.Capture.ResourceWorkers
	if concurrency == 0 {
		concurrency = captureConcurrency
	}
	limit := make(chan struct{}, concurrency)
	var captures sync.WaitGroup
	for i, job := range jobs {
		captures.Add(1)
		go func() {
			defer captures.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-captureCtx.Done():
				outcomes[i].err = captureCtx.Err()
				return
			}
			if reusable, ok := job.adapter.(catalog.ReusableAdapter); ok && req.Ledger != nil {
				result, captureErr := reusable.CaptureReusable(captureCtx, source.Scope, selections[i].Resource, job.options, catalog.ReuseRequest{
					Candidate:          job.candidate,
					InvalidationReason: job.invalidReason,
					Validate:           req.Ledger.ValidateArtifact,
					Materialize:        func(artifact captureledger.Artifact) error { return req.Ledger.Materialize(artifact, req.ArtifactRoot) },
				})
				outcomes[i].snapshot, outcomes[i].resource, outcomes[i].err = result.Snapshot, result.Resource, captureErr
			} else {
				outcomes[i].snapshot, outcomes[i].err = job.adapter.Capture(captureCtx, source.Scope, selections[i].Resource, job.options)
			}
			if outcomes[i].err != nil {
				cancelCaptures()
			}
		}()
	}
	captures.Wait()

	// Prefer a concrete adapter failure over cancellation induced by another
	// capture, while retaining selection order when several adapters fail.
	var captureErr error
	for _, outcome := range outcomes {
		if outcome.err == nil {
			continue
		}
		if captureErr == nil || (errors.Is(captureErr, context.Canceled) && !errors.Is(outcome.err, context.Canceled)) {
			captureErr = outcome.err
		}
	}
	if captureErr != nil {
		var diskErr *storage.InsufficientSpaceError
		if errors.As(captureErr, &diskErr) {
			return captureResult{}, &Error{Kind: ErrorFilesystem, Code: "DISK_SPACE_INSUFFICIENT", Message: diskErr.Error(), Remediation: "Free disk space, choose a larger --work-dir, or reduce the capture scope.", Err: captureErr}
		}
		return captureResult{}, sourceError(captureErr)
	}

	captured := captureResult{Plan: result}
	var snapshots []model.Snapshot
	dependenciesBySnapshot := make([][]model.Dependency, len(selections))
	for i, selection := range selections {
		adapter := jobs[i].adapter
		snapshot := outcomes[i].snapshot
		snapshot.Findings = append(snapshot.Findings, adapter.Validate(snapshot, model.Capabilities{FlociVersion: p.Target.FlociVersion})...)
		result.Selected = append(result.Selected, selection.Resource)
		result.Findings = append(result.Findings, snapshot.Findings...)
		deps := adapter.Dependencies(snapshot)
		result.Dependencies = append(result.Dependencies, deps...)
		dependenciesBySnapshot[i] = deps
		for _, art := range snapshot.Data {
			result.EstimatedBytes += art.Size
		}
		if snapshot.Dataset != nil {
			for _, chunk := range snapshot.Dataset.Chunks {
				result.EstimatedBytes += chunk.Data.Size
				if chunk.Index != nil {
					result.EstimatedBytes += chunk.Index.Size
				}
			}
		}
		snapshots = append(snapshots, *snapshot)
		if outcomes[i].resource != nil {
			captured.LedgerResources = append(captured.LedgerResources, *outcomes[i].resource)
			if jobs[i].generationID != "" {
				if captured.LedgerGenerations == nil {
					captured.LedgerGenerations = make(map[string]string)
				}
				descriptor := outcomes[i].resource.Descriptor
				captured.LedgerGenerations[resourceIdentityKey(descriptor.Service, descriptor.Type, descriptor.ID)] = jobs[i].generationID
			}
		}
		if result.Governance != nil {
			appendGovernanceAudit(result.Governance, policy, jobs[i].options.GovernanceAudit.Result())
		}
	}
	selected := make(map[string]model.ResourceRef, len(selections))
	for _, selection := range selections {
		selected[resourceIdentityKey(selection.Resource.Service, selection.Resource.Type, selection.Resource.ID)] = selection.Resource
	}
	if err := validateDependencyGraph(dependenciesBySnapshot, snapshots, selected); err != nil {
		return captureResult{}, &Error{Kind: ErrorPlan, Code: "DEPENDENCY_CYCLE", Message: err.Error(), Remediation: "Remove the cyclic dependency or capture only one side of the relationship.", Err: err}
	}
	for i := range snapshots {
		adapter := jobs[i].adapter
		var resolved, unresolved []model.Dependency
		for _, dependency := range dependenciesBySnapshot[i] {
			if dependencyResolved(dependency, selected) {
				resolved = append(resolved, dependency)
			} else {
				unresolved = append(unresolved, dependency)
			}
		}
		planningFindings, err := adapter.FinalizePlanning(&snapshots[i], unresolved)
		if err != nil {
			return captureResult{}, sourceError(err)
		}
		result.Findings = append(result.Findings, planningFindings...)
		result.Operations = append(result.Operations, operations(&snapshots[i], resolved)...)
	}
	if result.Governance != nil {
		sort.Slice(result.Governance.Rules, func(i, j int) bool { return result.Governance.Rules[i].RuleID < result.Governance.Rules[j].RuleID })
		sort.Slice(result.Governance.Cohorts, func(i, j int) bool {
			return result.Governance.Cohorts[i].ResourceIdentity < result.Governance.Cohorts[j].ResourceIdentity
		})
	}
	sortPlan(&result)
	captured.Plan = result
	captured.Snapshots = snapshots
	return captured, nil
}

func validateDependencyGraph(bySnapshot [][]model.Dependency, snapshots []model.Snapshot, selected map[string]model.ResourceRef) error {
	graph := make(map[string][]string)
	for i, dependencies := range bySnapshot {
		from := resourceIdentityKey(snapshots[i].Resource.Service, snapshots[i].Resource.Type, snapshots[i].Resource.ID)
		for _, dependency := range dependencies {
			if !dependency.Required || !dependencyResolved(dependency, selected) {
				continue
			}
			to := resourceIdentityKey(dependency.To.Service, dependency.To.Type, dependency.To.ID)
			graph[from] = append(graph[from], to)
		}
	}
	state := make(map[string]uint8)
	var visit func(string) bool
	visit = func(node string) bool {
		if state[node] == 1 {
			return true
		}
		if state[node] == 2 {
			return false
		}
		state[node] = 1
		for _, next := range graph[node] {
			if visit(next) {
				return true
			}
		}
		state[node] = 2
		return false
	}
	keys := make([]string, 0, len(graph))
	for key := range graph {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if visit(key) {
			return fmt.Errorf("required dependency graph contains a cycle involving %q", key)
		}
	}
	return nil
}

func dependencyResolved(dependency model.Dependency, selected map[string]model.ResourceRef) bool {
	candidate, ok := selected[resourceIdentityKey(dependency.To.Service, dependency.To.Type, dependency.To.ID)]
	return ok && dependency.To.ARN != "" && candidate.ARN != "" && dependency.To.ARN == candidate.ARN
}

func resourceIdentityKey(service, resourceType, id string) string {
	return service + "\x00" + resourceType + "\x00" + id
}

func governanceAuditForPolicy(policy *governance.EffectivePolicy) *model.GovernanceAudit {
	if policy == nil {
		return nil
	}
	audit := &model.GovernanceAudit{Profile: policy.Profile, PolicyIdentity: policy.Identity}
	for _, rule := range policy.Rules {
		audit.Rules = append(audit.Rules, model.GovernanceRuleAudit{RuleID: rule.ID, Action: string(rule.Action), Count: model.CountBucketZero})
		if rule.KeyID != "" {
			audit.KeyIDs = append(audit.KeyIDs, rule.KeyID)
		}
		if rule.Algorithm != "" {
			audit.Algorithms = append(audit.Algorithms, rule.Algorithm)
		}
	}
	for _, cohort := range policy.Cohorts {
		audit.KeyIDs = append(audit.KeyIDs, cohort.KeyID)
		audit.Algorithms = append(audit.Algorithms, cohort.Algorithm)
	}
	if len(policy.Cohorts) != 0 {
		digest := sha256.Sum256([]byte("floceed/governance/cohorts/v1\x00" + policy.Identity))
		audit.CohortIdentity = hex.EncodeToString(digest[:])
	}
	sort.Strings(audit.KeyIDs)
	audit.KeyIDs = slices.Compact(audit.KeyIDs)
	sort.Strings(audit.Algorithms)
	audit.Algorithms = slices.Compact(audit.Algorithms)
	return audit
}

func appendGovernanceAudit(destination *model.GovernanceAudit, policy *governance.EffectivePolicy, source governance.AuditSnapshot) {
	actions := make(map[string]string, len(policy.Rules))
	for _, rule := range policy.Rules {
		actions[rule.ID] = string(rule.Action)
	}
	for _, rule := range source.Rules {
		updated := false
		for index := range destination.Rules {
			if destination.Rules[index].RuleID == rule.RuleID {
				destination.Rules[index].Action = actions[rule.RuleID]
				destination.Rules[index].Count = rule.Count
				updated = true
				break
			}
		}
		if !updated {
			destination.Rules = append(destination.Rules, model.GovernanceRuleAudit{RuleID: rule.RuleID, Action: actions[rule.RuleID], Count: rule.Count})
		}
	}
	for _, cohort := range source.Cohorts {
		destination.Cohorts = append(destination.Cohorts, model.GovernanceCohortAudit{ResourceIdentity: cohort.ResourceIdentity, Eligible: cohort.Eligible, Retained: cohort.Retained, Truncated: cohort.Truncated})
	}
}

func operations(s *model.Snapshot, deps []model.Dependency) []model.Operation {
	base := s.Service + ":" + s.Resource.ID
	ops := []model.Operation{{ID: "base:" + base, Stage: model.StageBase, Service: s.Service, ResourceID: s.Resource.ID, Action: "ensure"}, {ID: "mutable:" + base, Stage: model.StageMutable, Service: s.Service, ResourceID: s.Resource.ID, Action: "apply", DependsOn: []string{"base:" + base}}}
	if len(deps) > 0 {
		dependsOn := []string{"mutable:" + base}
		for _, dependency := range deps {
			dependsOn = append(dependsOn, "mutable:"+dependency.To.Service+":"+dependency.To.ID)
		}
		sort.Strings(dependsOn)
		ops = append(ops, model.Operation{ID: "links:" + base, Stage: model.StageLinks, Service: s.Service, ResourceID: s.Resource.ID, Action: "link", DependsOn: dependsOn})
	}
	if len(s.Data) > 0 || (s.Dataset != nil && len(s.Dataset.Chunks) > 0) {
		ops = append(ops, model.Operation{ID: "data:" + base, Stage: model.StageData, Service: s.Service, ResourceID: s.Resource.ID, Action: "upsert", DependsOn: []string{"mutable:" + base}})
	}
	return ops
}

func sortPlan(p *Plan) {
	sort.Slice(p.Operations, func(i, j int) bool { return p.Operations[i].ID < p.Operations[j].ID })
	sort.Slice(p.Dependencies, func(i, j int) bool { return p.Dependencies[i].To.ARN < p.Dependencies[j].To.ARN })
	sort.Strings(p.RequiredIAMActions)
	p.RequiredIAMActions = slices.Compact(p.RequiredIAMActions)
	sortFindings(p.Findings)
}

func sortFindings(v []model.Finding) {
	sort.Slice(v, func(i, j int) bool {
		return cmp.Or(cmp.Compare(v[i].Code, v[j].Code), cmp.Compare(v[i].Resource, v[j].Resource)) < 0
	})
}

func s3Names(p config.Project) []string {
	v := make([]string, len(p.Resources.S3))
	for i, r := range p.Resources.S3 {
		v[i] = r.Name
	}
	return v
}

func ddbNames(p config.Project) []string {
	v := make([]string, len(p.Resources.DynamoDB))
	for i, r := range p.Resources.DynamoDB {
		v[i] = r.Name
	}
	return v
}
