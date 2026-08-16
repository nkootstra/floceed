package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"golang.org/x/sys/unix"
)

type PullOptions struct {
	WorkDir        string
	Restart        bool
	Progress       func(model.ProgressEvent)
	FixtureProfile string
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

func (a *Application) Pull(ctx context.Context, p config.Project, projectDir, profile, region string) (model.Manifest, error) {
	return a.PullWithOptions(ctx, p, projectDir, profile, region, PullOptions{})
}

func (a *Application) PullWithOptions(ctx context.Context, p config.Project, projectDir, profile, region string, options PullOptions) (model.Manifest, error) {
	if err := p.Validate(); err != nil {
		return model.Manifest{}, &Error{Kind: ErrorPlan, Code: "PROJECT_INVALID", Message: err.Error(), Err: err}
	}
	policy, err := resolveGovernance(p, options.FixtureProfile)
	if err != nil {
		return model.Manifest{}, err
	}
	workBase := options.WorkDir
	if workBase == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return model.Manifest{}, filesystemError(err)
		}
		workBase = filepath.Join(cache, "floceed", "captures")
	}
	abs, err := filepath.Abs(projectDir)
	if err != nil {
		return model.Manifest{}, filesystemError(err)
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
		return model.Manifest{}, sourceError(err)
	}
	fingerprint := captureFingerprint(p, abs, effectiveProfile, effectiveRegion, source.Identity.AccountID, policy)
	tmp := filepath.Join(workBase, hex.EncodeToString(fingerprint[:16]))
	if err := os.MkdirAll(workBase, 0o700); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	lockPath := tmp + ".lock"
	release, err := acquireCaptureLock(lockPath)
	if err != nil {
		if errors.Is(err, errCaptureLocked) {
			return model.Manifest{}, &Error{Kind: ErrorFilesystem, Code: "CAPTURE_IN_PROGRESS", Message: "another capture is using this checkpoint", Remediation: "Wait for the other floceed pull to finish or stop it before retrying.", Err: err}
		}
		return model.Manifest{}, filesystemError(err)
	}
	defer release()
	if options.Restart {
		if err := os.RemoveAll(tmp); err != nil {
			return model.Manifest{}, filesystemError(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(tmp, "artifacts"), 0o700); err != nil {
		return model.Manifest{}, filesystemError(err)
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
		options.Progress(event)
	}
	report(model.ProgressEvent{Operation: "pull", Phase: "prepare", Message: "preparing capture"})
	planned, snapshots, err := a.capture(ctx, captureRequest{
		Project:        p,
		Profile:        profile,
		Governance:     policy,
		Region:         region,
		ArtifactRoot:   filepath.Join(tmp, "artifacts"),
		IncludeData:    true,
		CheckpointRoot: filepath.Join(tmp, "checkpoints"),
		Progress:       report,
		Source:         &source,
	})
	if err != nil {
		return model.Manifest{}, err
	}
	manifest := a.manifest(p, planned, snapshots)
	target := filepath.Join(projectDir, filepath.FromSlash(p.Output.Directory))
	report(model.ProgressEvent{Operation: "pull", Phase: "install", Message: "validating and installing bundle"})
	if err := bundle.Render(ctx, target, p, manifest, bundle.RenderOptions{ArtifactRoot: filepath.Join(tmp, "artifacts"), ValidateCompose: a.ComposeValidator}); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	if err := os.RemoveAll(tmp); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	report(model.ProgressEvent{Operation: "pull", Phase: "complete", Message: "bundle installed"})
	return manifest, nil
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
