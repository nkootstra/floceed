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
	ledgerResources    []captureledger.Resource
	ledgerGenerations  map[string]string
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
	planned, _, err := a.capture(ctx, captureRequest{Project: p, Profile: options.AWSProfile, Region: options.Region, Governance: policy})
	return planned, err
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

func (a *Application) capture(ctx context.Context, req captureRequest) (Plan, []model.Snapshot, error) {
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
			return Plan{}, nil, sourceError(err)
		}
	}
	if p.Source.ExpectedAccountID != "" && p.Source.ExpectedAccountID != source.Identity.AccountID {
		return Plan{}, nil, &Error{Kind: ErrorSource, Code: "SOURCE_ACCOUNT_MISMATCH", Message: fmt.Sprintf("AWS profile resolved to account %s, expected %s", source.Identity.AccountID, p.Source.ExpectedAccountID)}
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
		candidate     []captureledger.Resource
		invalidReason captureledger.Reason
		generationID  string
	}
	jobs := make([]captureJob, len(selections))
	for i, selection := range selections {
		adapter, ok := source.Registry.Get(selection.Resource.Service)
		if !ok {
			return Plan{}, nil, &Error{Kind: ErrorPlan, Code: "ADAPTER_MISSING", Message: "no adapter for " + selection.Resource.Service}
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
						job.candidate = append(job.candidate, resource)
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
					Candidates:         job.candidate,
					InvalidationReason: job.invalidReason,
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
			return Plan{}, nil, &Error{Kind: ErrorFilesystem, Code: "DISK_SPACE_INSUFFICIENT", Message: diskErr.Error(), Remediation: "Free disk space, choose a larger --work-dir, or reduce the capture scope.", Err: captureErr}
		}
		return Plan{}, nil, sourceError(captureErr)
	}

	var snapshots []model.Snapshot
	for i, selection := range selections {
		adapter := jobs[i].adapter
		snapshot := outcomes[i].snapshot
		snapshot.Findings = append(snapshot.Findings, adapter.Validate(snapshot, model.Capabilities{FlociVersion: p.Target.FlociVersion})...)
		result.Selected = append(result.Selected, selection.Resource)
		result.Findings = append(result.Findings, snapshot.Findings...)
		deps := adapter.Dependencies(snapshot)
		planningFindings, err := adapter.FinalizePlanning(snapshot, deps)
		if err != nil {
			return Plan{}, nil, sourceError(err)
		}
		result.Findings = append(result.Findings, planningFindings...)
		result.Dependencies = append(result.Dependencies, deps...)
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
		result.Operations = append(result.Operations, operations(snapshot, deps)...)
		snapshots = append(snapshots, *snapshot)
		if outcomes[i].resource != nil {
			result.ledgerResources = append(result.ledgerResources, *outcomes[i].resource)
			if jobs[i].generationID != "" {
				if result.ledgerGenerations == nil {
					result.ledgerGenerations = make(map[string]string)
				}
				descriptor := outcomes[i].resource.Descriptor
				result.ledgerGenerations[descriptor.Service+"\x00"+descriptor.Type+"\x00"+descriptor.ID] = jobs[i].generationID
			}
		}
		if result.Governance != nil {
			appendGovernanceAudit(result.Governance, policy, jobs[i].options.GovernanceAudit.Result())
		}
	}
	if result.Governance != nil {
		sort.Slice(result.Governance.Rules, func(i, j int) bool { return result.Governance.Rules[i].RuleID < result.Governance.Rules[j].RuleID })
		sort.Slice(result.Governance.Cohorts, func(i, j int) bool {
			return result.Governance.Cohorts[i].ResourceIdentity < result.Governance.Cohorts[j].ResourceIdentity
		})
	}
	sortPlan(&result)
	return result, snapshots, nil
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
		ops = append(ops, model.Operation{ID: "links:" + base, Stage: model.StageLinks, Service: s.Service, ResourceID: s.Resource.ID, Action: "link", DependsOn: []string{"mutable:" + base}})
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
