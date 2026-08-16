package app

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

const captureConcurrency = 4

type Plan struct {
	Source             model.SourceMetadata `json:"source"`
	Selected           []model.ResourceRef  `json:"selected"`
	Operations         []model.Operation    `json:"operations"`
	Dependencies       []model.Dependency   `json:"dependencies,omitempty"`
	Findings           []model.Finding      `json:"findings,omitempty"`
	EstimatedBytes     int64                `json:"estimated_bytes"`
	RequiredIAMActions []string             `json:"required_iam_actions"`
}

func (a *Application) Plan(ctx context.Context, p config.Project, profile, region string) (Plan, error) {
	planned, _, err := a.capture(ctx, captureRequest{Project: p, Profile: profile, Region: region})
	return planned, err
}

type captureRequest struct {
	Project        config.Project
	Profile        string
	Region         string
	ArtifactRoot   string
	IncludeData    bool
	CheckpointRoot string
	Progress       func(model.ProgressEvent)
	Source         *Source
}

func (a *Application) capture(ctx context.Context, req captureRequest) (Plan, []model.Snapshot, error) {
	p, profile, region := req.Project, req.Profile, req.Region
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
	result := Plan{Source: model.SourceMetadata{AccountID: source.Identity.AccountID, Region: region}, RequiredIAMActions: []string{"sts:GetCallerIdentity"}}
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
		adapter catalog.Adapter
		options model.CaptureOptions
	}
	jobs := make([]captureJob, len(selections))
	for i, selection := range selections {
		adapter, ok := source.Registry.Get(selection.Resource.Service)
		if !ok {
			return Plan{}, nil, &Error{Kind: ErrorPlan, Code: "ADAPTER_MISSING", Message: "no adapter for " + selection.Resource.Service}
		}
		options := selection.Options
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
		jobs[i] = captureJob{adapter: adapter, options: options}
	}

	type captureOutcome struct {
		snapshot *model.Snapshot
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
			outcomes[i].snapshot, outcomes[i].err = job.adapter.Capture(captureCtx, source.Scope, selections[i].Resource, job.options)
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
	}
	sortPlan(&result)
	return result, snapshots, nil
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
