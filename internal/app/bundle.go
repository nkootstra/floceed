package app

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/governance"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
	"golang.org/x/sys/unix"
)

type PullOptions struct {
	WorkDir        string
	Restart        bool
	Progress       func(model.ProgressEvent)
	FixtureProfile string
}

type BaselineState string

const (
	BaselineAbsent  BaselineState = "absent"
	BaselinePresent BaselineState = "present"
)

// PullResult describes the bundle that was installed and, when replacing an
// existing bundle, its disclosure-safe semantic effect.
type PullResult struct {
	model.Manifest
	Baseline BaselineState       `json:"baseline"`
	Receipt  *inspection.Receipt `json:"receipt,omitempty"`
}

var errCaptureLocked = errors.New("capture checkpoint is locked by another process")

// acquireCaptureLock takes a non-blocking advisory flock on lockPath. flock is
// released by the kernel when the holder exits (including SIGKILL), so a crashed
// pull can never leave a stale lock that blocks future captures. The lock file
// itself is intentionally never removed: deleting a lock file while another
// process holds the flock on its inode would let a third process lock a new
// inode concurrently.
func acquireCaptureLock(lockPath string) (release func(), err error) {
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", errCaptureLocked, lockPath)
		}
		return nil, err
	}
	_, _ = lock.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	return func() {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		_ = lock.Close()
	}, nil
}

func (a *Application) Pull(ctx context.Context, p config.Project, projectDir, profile, region string) (PullResult, error) {
	return a.PullWithOptions(ctx, p, projectDir, profile, region, PullOptions{})
}

func (a *Application) PullWithOptions(ctx context.Context, p config.Project, projectDir, profile, region string, options PullOptions) (PullResult, error) {
	if err := ctx.Err(); err != nil {
		return PullResult{}, inspectError(err)
	}
	if err := p.Validate(); err != nil {
		return PullResult{}, &Error{Kind: ErrorPlan, Code: "PROJECT_INVALID", Message: err.Error(), Err: err}
	}
	policy, err := resolveGovernance(p, options.FixtureProfile)
	if err != nil {
		return PullResult{}, err
	}
	target := filepath.Join(projectDir, filepath.FromSlash(p.Output.Directory))
	baselineState, baselineProjection, err := loadPullBaseline(ctx, target)
	if err != nil {
		return PullResult{}, err
	}
	workBase := options.WorkDir
	if workBase == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return PullResult{}, filesystemError(err)
		}
		workBase = filepath.Join(cache, "floceed", "captures")
	}
	abs, err := filepath.Abs(projectDir)
	if err != nil {
		return PullResult{}, filesystemError(err)
	}
	effectiveProfile, effectiveRegion := profile, region
	if effectiveProfile == "" {
		effectiveProfile = p.Source.Profile
	}
	if effectiveRegion == "" {
		effectiveRegion = p.Source.Region
	}
	source, err := a.Factory.Open(ctx, SourceRequest{Profile: effectiveProfile, Region: effectiveRegion, S3Names: s3Names(p), DynamoDBNames: ddbNames(p)})
	if err != nil {
		return PullResult{}, sourceError(err)
	}
	fingerprint := captureFingerprint(p, abs, effectiveProfile, effectiveRegion, source.Identity.AccountID, policy)
	tmp := filepath.Join(workBase, hex.EncodeToString(fingerprint[:16]))
	if err := os.MkdirAll(workBase, 0o700); err != nil {
		return PullResult{}, filesystemError(err)
	}
	ledger, err := captureledger.OpenStore(filepath.Join(workBase, "ledger"))
	if err != nil {
		return PullResult{}, filesystemError(err)
	}
	lockPath := tmp + ".lock"
	release, err := acquireCaptureLock(lockPath)
	if err != nil {
		if errors.Is(err, errCaptureLocked) {
			return PullResult{}, &Error{Kind: ErrorFilesystem, Code: "CAPTURE_IN_PROGRESS", Message: "another capture is using this checkpoint", Remediation: "Wait for the other floceed pull to finish or stop it before retrying.", Err: err}
		}
		return PullResult{}, filesystemError(err)
	}
	defer release()
	if options.Restart {
		if err := os.RemoveAll(tmp); err != nil {
			return PullResult{}, filesystemError(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(tmp, "artifacts"), 0o700); err != nil {
		return PullResult{}, filesystemError(err)
	}
	var progressMu sync.Mutex
	var sequence int64
	report := func(event model.ProgressEvent) {
		if options.Progress == nil {
			return
		}
		progressMu.Lock()
		defer progressMu.Unlock()
		sequence++
		event.SchemaVersion = 1
		event.Event = "progress"
		event.Sequence = sequence
		if event.TotalRecords > event.CompletedRecords {
			event.RemainingRecords = event.TotalRecords - event.CompletedRecords
		}
		if event.TotalBytes > event.CompletedBytes {
			event.RemainingBytes = event.TotalBytes - event.CompletedBytes
		}
		options.Progress(event)
	}
	report(model.ProgressEvent{Operation: "pull", Phase: "prepare", Message: "preparing capture"})
	captured, err := a.capture(ctx, captureRequest{
		Project:        p,
		Profile:        profile,
		Governance:     policy,
		Region:         region,
		ArtifactRoot:   filepath.Join(tmp, "artifacts"),
		IncludeData:    true,
		CheckpointRoot: filepath.Join(tmp, "checkpoints"),
		Progress:       report,
		Source:         &source,
		Ledger:         ledger,
		LedgerSource:   captureledger.SourceIdentity{AccountID: source.Identity.AccountID, Region: effectiveRegion},
	})
	if err != nil {
		return PullResult{}, err
	}
	planned, snapshots := captured.Plan, captured.Snapshots
	manifest := a.manifest(p, planned, snapshots)
	currentProjection, err := inspection.ProjectManifest(manifest)
	if err != nil {
		return PullResult{}, &Error{Kind: ErrorPlan, Code: "MANIFEST_INVALID", Message: err.Error(), Err: err}
	}
	ledgerGeneration, err := a.buildLedgerGeneration(captured, source.Identity.AccountID, effectiveRegion)
	if err != nil {
		return PullResult{}, err
	}
	report(model.ProgressEvent{Operation: "pull", Phase: "install", Message: "validating and installing bundle"})
	beforeInstall := func() error {
		generated, loadErr := bundle.LoadGenerated(ctx, target)
		if loadErr != nil {
			if errors.Is(loadErr, bundle.ErrGeneratedRootMissing) {
				baselineState = BaselineAbsent
				baselineProjection = inspection.Projection{}
				return nil
			}
			return invalidBaselineError(loadErr)
		}
		projection, projectErr := inspection.ProjectManifest(generated.Manifest)
		if projectErr != nil {
			return inspectError(projectErr)
		}
		baselineState = BaselinePresent
		baselineProjection = projection
		return nil
	}
	if err := bundle.Render(ctx, target, p, manifest, bundle.RenderOptions{ArtifactRoot: filepath.Join(tmp, "artifacts"), ValidateCompose: a.ComposeValidator, BeforeInstall: beforeInstall}); err != nil {
		var appErr *Error
		if errors.As(err, &appErr) {
			return PullResult{}, appErr
		}
		return PullResult{}, filesystemError(err)
	}
	if ledgerGeneration != nil {
		if err := a.publishLedger(ledger, *ledgerGeneration, filepath.Join(tmp, "artifacts")); err != nil {
			report(model.ProgressEvent{Operation: "pull", Phase: "ledger", Message: "bundle installed; capture reuse cache was not updated"})
		}
	}
	if err := os.RemoveAll(tmp); err != nil {
		return PullResult{}, filesystemError(err)
	}
	report(model.ProgressEvent{Operation: "pull", Phase: "complete", Message: "bundle installed"})
	result := PullResult{Manifest: manifest, Baseline: baselineState}
	if baselineState == BaselinePresent {
		receipt := inspection.Compare(baselineProjection, currentProjection)
		if ledgerGeneration != nil {
			attachLedgerDecisions(&receipt, *ledgerGeneration, captured.LedgerGenerations)
		}
		result.Receipt = &receipt
	}
	return result, nil
}

// loadPullBaseline validates the installed bundle and projects it for the
// semantic comparison receipt. An absent target is the "first pull" case.
func loadPullBaseline(ctx context.Context, target string) (BaselineState, inspection.Projection, error) {
	if _, statErr := os.Stat(target); statErr == nil {
		generated, loadErr := bundle.LoadGenerated(ctx, target)
		if loadErr != nil {
			return BaselineAbsent, inspection.Projection{}, invalidBaselineError(loadErr)
		}
		projection, loadErr := inspection.ProjectManifest(generated.Manifest)
		if loadErr != nil {
			return BaselineAbsent, inspection.Projection{}, inspectError(loadErr)
		}
		return BaselinePresent, projection, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return BaselineAbsent, inspection.Projection{}, inspectError(statErr)
	}
	return BaselineAbsent, inspection.Projection{}, nil
}

// buildLedgerGeneration wraps captured ledger resources in a completed
// generation ready for publication, when any reusable unit was captured.
func (a *Application) buildLedgerGeneration(captured captureResult, accountID, region string) (*captureledger.Generation, error) {
	if len(captured.LedgerResources) == 0 {
		return nil, nil
	}
	completedAt := time.Now().UTC()
	if a.Now != nil {
		completedAt = a.Now().UTC()
	}
	generation := captureledger.Generation{
		SchemaVersion: captureledger.CurrentSchemaVersion,
		Source:        captureledger.SourceIdentity{AccountID: accountID, Region: region},
		CreatedAt:     completedAt,
		CompletedAt:   completedAt,
		Resources:     captured.LedgerResources,
	}
	generation.ID = ledgerGenerationID(generation)
	if _, err := generation.CanonicalJSON(); err != nil {
		return nil, &Error{Kind: ErrorPlan, Code: "CAPTURE_LEDGER_INVALID", Message: err.Error(), Err: err}
	}
	return &generation, nil
}

func attachLedgerDecisions(receipt *inspection.Receipt, generation captureledger.Generation, previous map[string]string) {
	changes := make(map[string]*inspection.ResourceChange, len(receipt.Resources))
	for index := range receipt.Resources {
		identity := receipt.Resources[index].Resource
		changes[resourceIdentityKey(identity.Service, identity.Type, identity.ID)] = &receipt.Resources[index]
	}
	for _, resource := range generation.Resources {
		key := resourceIdentityKey(resource.Descriptor.Service, resource.Descriptor.Type, resource.Descriptor.ID)
		change := changes[key]
		if change == nil { // Defensive: ledger resources are selected manifest resources.
			continue
		}
		for _, unit := range resource.Units {
			decision := inspection.UnitDecision{
				ID: unit.ID, Outcome: string(unit.Outcome), Reason: string(unit.Reason),
				FreshnessDigest: unit.Freshness.Digest, ArtifactCount: len(unit.Artifacts), Generation: generation.ID, PreviousGeneration: previous[key],
			}
			for _, artifact := range unit.Artifacts {
				decision.ArtifactBytes += artifact.Size
			}
			change.Units = append(change.Units, decision)
		}
		sort.Slice(change.Units, func(i, j int) bool {
			return cmp.Or(cmp.Compare(change.Units[i].ID, change.Units[j].ID), cmp.Compare(change.Units[i].Outcome, change.Units[j].Outcome), cmp.Compare(change.Units[i].Reason, change.Units[j].Reason)) < 0
		})
	}
}

func ledgerGenerationID(generation captureledger.Generation) string {
	payload, _ := json.Marshal(struct {
		Source      captureledger.SourceIdentity
		CompletedAt time.Time
		Resources   []captureledger.Resource
	}{generation.Source, generation.CompletedAt, generation.Resources})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func invalidBaselineError(err error) error {
	if errors.Is(err, bundle.ErrGeneratedRootMissing) {
		return inspectError(err)
	}
	return &Error{Kind: ErrorFilesystem, Code: "BUNDLE_INTEGRITY_INVALID", Message: fmt.Sprintf("installed baseline is invalid: %v", err), Remediation: "Repair or remove the invalid generated bundle before pulling again.", Err: err}
}

func captureFingerprint(p config.Project, directory, profile, region, accountID string, policy *governance.EffectivePolicy) [32]byte {
	payload, _ := json.Marshal(struct {
		Project                                           config.Project
		Directory, Profile, Region, AccountID, Governance string
	}{p, directory, profile, region, accountID, governance.IdentityOf(policy)})
	return sha256.Sum256(payload)
}

func (a *Application) Render(ctx context.Context, p config.Project, projectDir string) (model.Manifest, error) {
	target := filepath.Join(projectDir, filepath.FromSlash(p.Output.Directory))
	b, err := os.ReadFile(filepath.Join(target, "bundle", "manifest.json"))
	if err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	var m model.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	if err := m.Validate(); err != nil {
		return model.Manifest{}, &Error{Kind: ErrorPlan, Code: "MANIFEST_INVALID", Message: err.Error(), Err: err}
	}
	if err := bundle.ValidateGenerated(target); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	if err := bundle.Render(ctx, target, p, m, bundle.RenderOptions{ArtifactRoot: target, ValidateCompose: a.ComposeValidator}); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	return m, nil
}

func (a *Application) manifest(p config.Project, planned Plan, snapshots []model.Snapshot) model.Manifest {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	version := a.Version
	if version == "" {
		version = "dev"
	}
	partial := false
	for _, finding := range planned.Findings {
		partial = partial || finding.Code == "DATA_CAPTURE_PARTIAL"
	}
	return model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Tool: model.ToolMetadata{Version: version}, Target: model.TargetMetadata{FlociVersion: p.Target.FlociVersion, Image: compose.Image}, Source: planned.Source, Capture: model.CaptureMetadata{CapturedAt: now().UTC(), Partial: partial}, Selected: planned.Selected, Snapshots: snapshots, Operations: planned.Operations, Findings: planned.Findings, Governance: planned.Governance}
}
