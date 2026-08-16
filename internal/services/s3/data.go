package s3

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"mime"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

const s3PackBytes int64 = 256 << 20
const s3PackObjects = 10000

const (
	s3PreflightBaseOverhead      int64 = 1 << 20
	s3PreflightPerObjectOverhead int64 = 64 << 10
)

var requireS3Available = storage.RequireAvailable

type inventoryEntry struct {
	Key  string `json:"key"`
	ETag string `json:"etag,omitempty"`
	Size int64  `json:"size"`
}

type captureGovernance struct {
	body     *governance.Rule
	metadata map[string][]governance.Rule
	engine   *governance.Engine
}

func newCaptureGovernance(bucket string, policy *governance.EffectivePolicy) *captureGovernance {
	compiled := &captureGovernance{metadata: make(map[string][]governance.Rule)}
	if policy == nil {
		return compiled
	}
	compiled.engine = governance.NewEngine(policy.Profile, policy.Secret())
	for _, rule := range policy.Rules {
		if rule.Service != governance.ServiceS3 || rule.Resource != bucket {
			continue
		}
		switch rule.Target.Kind {
		case governance.TargetS3TextBody:
			if compiled.body == nil {
				copy := rule
				compiled.body = &copy
			}
		case governance.TargetS3Metadata:
			key := strings.ToLower(rule.Target.Path)
			compiled.metadata[key] = append(compiled.metadata[key], rule)
		}
	}
	return compiled
}

// s3CheckpointVersion 3 also records the effective governance policy identity
// so transformed and untransformed chunks can never be mixed on resume.
const s3CheckpointVersion = 3

type s3Checkpoint struct {
	Version           int               `json:"version"`
	Bucket            string            `json:"bucket"`
	Mode              string            `json:"mode"`
	Prefixes          []string          `json:"prefixes"`
	MaxObjects        int               `json:"max_objects,omitempty"`
	MaxObjectBytes    int64             `json:"max_object_bytes,omitempty"`
	MaxTotalBytes     int64             `json:"max_total_bytes,omitempty"`
	Overwrite         string            `json:"overwrite,omitempty"`
	PolicyIdentity    string            `json:"policy_identity,omitempty"`
	Prefix            int               `json:"prefix"`
	Token             string            `json:"token,omitempty"`
	InventoryBytes    int64             `json:"inventory_bytes"`
	InventoryComplete bool              `json:"inventory_complete"`
	Records           int64             `json:"records"`
	SourceBytes       int64             `json:"source_bytes"`
	ProcessedOffset   int64             `json:"processed_offset"`
	ProcessedRecords  int64             `json:"processed_records"`
	Chunks            []model.DataChunk `json:"chunks,omitempty"`
	GovernanceCounts  map[string]int    `json:"governance_rule_counts,omitempty"`
}

func (a *Adapter) captureObjects(ctx context.Context, bucket string, b *Bucket, snap *model.Snapshot, opts model.CaptureOptions) error {
	_, err := a.captureObjectsReusable(ctx, model.SourceScope{}, model.ResourceRef{Service: "s3", Type: "bucket", ID: bucket}, b, snap, opts, catalog.ReuseRequest{})
	return err
}

func (a *Adapter) captureObjectsReusable(ctx context.Context, scope model.SourceScope, ref model.ResourceRef, b *Bucket, snap *model.Snapshot, opts model.CaptureOptions, reuse catalog.ReuseRequest) (*captureledger.Resource, error) {
	bucket := ref.ID
	full := opts.Mode == "full"
	if !full && (opts.Limits.MaxObjects <= 0 || opts.Limits.MaxObjectBytes <= 0 || opts.Limits.MaxTotalBytes <= 0) {
		return nil, fmt.Errorf("S3 data limits must all be positive")
	}
	prefixes := append([]string(nil), opts.Prefixes...)
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}
	sort.Strings(prefixes)
	prefixes = slices.Compact(prefixes)
	// Sorted, non-overlapping prefixes preserve S3's lexical page order without
	// retaining an unbounded inventory before applying fixture limits.
	prefixes = compactPrefixes(prefixes)
	work := opts.CheckpointDirectory
	removeWork := false
	if work == "" {
		var err error
		work, err = os.MkdirTemp("", "floceed-s3-")
		if err != nil {
			return nil, err
		}
		removeWork = true
	}
	if removeWork {
		defer os.RemoveAll(work)
	}
	if err := os.MkdirAll(work, 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.ArtifactDirectory, 0o700); err != nil {
		return nil, err
	}
	cpPath := filepath.Join(work, "checkpoint.json")
	invPath := filepath.Join(work, "inventory.ndjson")
	cp, resumed, err := loadS3Checkpoint(cpPath, bucket, opts, prefixes)
	if err != nil {
		return nil, err
	}
	opts.GovernanceAudit.RestoreRuleCounts(cp.GovernanceCounts)
	compiledGovernance := newCaptureGovernance(bucket, opts.Governance)
	// Integrity verification is O(total previously captured data) on every
	// resume; the checkpoint only references chunks that were fully written
	// and fsynced, so a mismatch means the capture is not resumable.
	for _, chunk := range cp.Chunks {
		refs := []model.ArtifactRef{chunk.Data}
		if chunk.Index != nil {
			refs = append(refs, *chunk.Index)
		}
		for _, ref := range refs {
			sum, e := bundle.SumFile(filepath.Join(opts.ArtifactDirectory, filepath.FromSlash(ref.Path)))
			if e != nil || sum.SHA256 != ref.SHA256 || sum.Size != ref.Size {
				return nil, fmt.Errorf("corrupt S3 capture checkpoint; restart the capture")
			}
		}
	}
	if !cp.InventoryComplete {
		truncatedCapture, err := a.buildInventory(ctx, bucket, prefixes, full, opts, invPath, cpPath, &cp, snap)
		if err != nil {
			return nil, err
		}
		if truncatedCapture {
			cp.InventoryComplete = true
			if err := saveS3Checkpoint(cpPath, cp); err != nil {
				return nil, err
			}
		}
	}
	if full {
		remaining, err := estimateS3RemainingArtifactBytes(invPath, cp.ProcessedOffset, compiledGovernance)
		if err != nil {
			return nil, err
		}
		if err := requireS3Available(opts.ArtifactDirectory, remaining, 1); err != nil {
			return nil, err
		}
	}
	if opts.Progress != nil {
		opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "s3", Resource: bucket, CompletedRecords: cp.ProcessedRecords, TotalRecords: cp.Records, CompletedBytes: chunkBytes(cp.Chunks), TotalBytes: cp.SourceBytes, TotalPrecision: "exact", Resumed: resumed})
	}
	dataset := model.Dataset{Format: "s3-tar-gzip-v1", Records: cp.ProcessedRecords, SourceBytes: cp.SourceBytes, Consistency: "best_effort", Resumed: resumed, Chunks: append([]model.DataChunk(nil), cp.Chunks...)}
	inv, err := os.Open(invPath)
	if err != nil {
		return nil, err
	}
	defer inv.Close()
	definition, definitionErr := s3CaptureDefinition(scope, ref, opts)
	if definitionErr != nil && reuse.Materialize != nil {
		return nil, definitionErr
	}
	var candidate *captureledger.Resource
	for i := range reuse.Candidates {
		if reuse.Candidates[i].CaptureDefinition == definition {
			candidate = &reuse.Candidates[i]
			break
		}
	}
	candidateUnits := map[string]captureledger.Unit{}
	if candidate != nil {
		for _, unit := range candidate.Units {
			if strings.HasPrefix(unit.ID, "pack-") {
				candidateUnits[unit.ID] = unit
			}
		}
	}
	resource := &captureledger.Resource{Descriptor: captureledger.ResourceDescriptor{Service: ref.Service, Type: ref.Type, ID: ref.ID}, CaptureDefinition: definition}
	currentIdentities := map[string]struct{}{}
	if err := scanS3InventoryPacks(invPath, func(number int, entries []inventoryEntry) error {
		freshness := s3PackFreshness(entries)
		for identity := range freshness.Components {
			currentIdentities[identity] = struct{}{}
		}
		if number <= len(cp.Chunks) {
			resource.Units = append(resource.Units, captureledger.Unit{ID: fmt.Sprintf("pack-%06d", number), Freshness: freshness, Artifacts: chunkArtifacts(cp.Chunks[number-1]), Outcome: captureledger.UnitOutcomeRefreshed, Reason: captureledger.ReasonNoCandidate, CapturedAt: time.Now().UTC()})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if _, err = inv.Seek(cp.ProcessedOffset, io.SeekStart); err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(inv, 1<<20)
	for cp.ProcessedRecords < cp.Records {
		var entries []inventoryEntry
		var linesBytes, packBytes int64
		for len(entries) < s3PackObjects {
			line, e := reader.ReadBytes('\n')
			if e != nil && e != io.EOF {
				return nil, e
			}
			if len(line) == 0 {
				break
			}
			var entry inventoryEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				return nil, err
			}
			if len(entries) > 0 && packBytes+entry.Size > s3PackBytes {
				if _, err := inv.Seek(cp.ProcessedOffset+linesBytes, io.SeekStart); err != nil {
					return nil, err
				}
				reader.Reset(inv)
				break
			}
			entries = append(entries, entry)
			linesBytes += int64(len(line))
			packBytes += entry.Size
			if e == io.EOF {
				break
			}
		}
		if len(entries) == 0 {
			break
		}
		number := len(dataset.Chunks) + 1
		unitID := fmt.Sprintf("pack-%06d", number)
		freshness := s3PackFreshness(entries)
		unit, found := candidateUnits[unitID]
		reason := captureledger.ReasonNoCandidate
		if candidate == nil && len(reuse.Candidates) != 0 {
			reason = captureledger.ReasonCaptureDefinitionChanged
		} else if reuse.InvalidationReason != "" {
			reason = reuse.InvalidationReason
		} else if found && unit.Outcome == captureledger.UnitOutcomeInvalidated {
			reason = unit.Reason
		} else if found {
			reason = captureledger.ReasonSourceContentChanged
		}
		var chunk model.DataChunk
		reused := found && unit.Outcome != captureledger.UnitOutcomeInvalidated && unit.Freshness.Kind == freshness.Kind && unit.Freshness.Digest == freshness.Digest && maps.Equal(unit.Freshness.Components, freshness.Components) && len(unit.Artifacts) == 2 && reuse.Materialize != nil
		if reused {
			for _, artifact := range unit.Artifacts {
				if err := reuse.Materialize(artifact); err != nil {
					reused = false
					if classified, ok := captureledger.InvalidationReason(err); ok {
						reason = classified
					} else {
						reason = captureledger.ReasonArtifactCorrupt
					}
					break
				}
			}
		}
		if reused {
			data, index := ledgerArtifactRef(unit.Artifacts[0]), ledgerArtifactRef(unit.Artifacts[1])
			chunk = model.DataChunk{Data: data, Index: &index, Records: int64(len(entries)), SourceBytes: inventorySize(entries)}
			unit.Outcome, unit.Reason, unit.Freshness = captureledger.UnitOutcomeReused, captureledger.ReasonReused, freshness
		} else {
			chunk, err = a.capturePack(ctx, bucket, number, entries, opts, compiledGovernance, snap)
			if err != nil {
				return nil, err
			}
			unit = captureledger.Unit{ID: unitID, Freshness: freshness, Artifacts: chunkArtifacts(chunk), Outcome: captureledger.UnitOutcomeRefreshed, Reason: reason, CapturedAt: time.Now().UTC()}
		}
		delete(candidateUnits, unitID)
		resource.Units = append(resource.Units, unit)
		cp.Chunks = append(cp.Chunks, chunk)
		cp.ProcessedRecords += int64(len(entries))
		cp.ProcessedOffset += linesBytes
		cp.GovernanceCounts = opts.GovernanceAudit.RuleCounts()
		if err := saveS3Checkpoint(cpPath, cp); err != nil {
			return nil, err
		}
		dataset.Chunks = append(dataset.Chunks, chunk)
		dataset.Records = cp.ProcessedRecords
		if opts.Progress != nil {
			opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "s3", Resource: bucket, CompletedRecords: cp.ProcessedRecords, TotalRecords: cp.Records, CompletedBytes: chunkBytes(cp.Chunks), TotalBytes: cp.SourceBytes, CompletedChunks: int64(len(cp.Chunks)), TotalPrecision: "exact", Resumed: resumed})
		}
	}
	if candidate != nil {
		resource.Units = append(resource.Units, missingS3Units(*candidate, currentIdentities)...)
	}
	sort.Slice(resource.Units, func(i, j int) bool { return resource.Units[i].ID < resource.Units[j].ID })
	b.Objects = nil
	snap.Dataset = &dataset
	return resource, nil
}

func missingS3Units(candidate captureledger.Resource, current map[string]struct{}) []captureledger.Unit {
	missing := map[string]captureledger.Unit{}
	for _, unit := range candidate.Units {
		if !strings.HasPrefix(unit.ID, "pack-") {
			continue
		}
		for identity, digest := range unit.Freshness.Components {
			if _, exists := current[identity]; exists {
				continue
			}
			missing[identity] = captureledger.Unit{ID: "object-" + identity, Freshness: captureledger.FreshnessEvidence{Kind: "s3_inventory_object_v1", Digest: digest}, Outcome: captureledger.UnitOutcomeInvalidated, Reason: captureledger.ReasonSourceUnitMissing, CapturedAt: unit.CapturedAt}
		}
	}
	result := make([]captureledger.Unit, 0, len(missing))
	for _, unit := range missing {
		result = append(result, unit)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func scanS3InventoryPacks(path string, visit func(int, []inventoryEntry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var entries []inventoryEntry
	var packBytes int64
	number := 1
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		if err := visit(number, entries); err != nil {
			return err
		}
		number++
		entries, packBytes = nil, 0
		return nil
	}
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		if len(line) != 0 {
			var entry inventoryEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				return err
			}
			if len(entries) >= s3PackObjects || (len(entries) > 0 && packBytes+entry.Size > s3PackBytes) {
				if err := flush(); err != nil {
					return err
				}
			}
			entries = append(entries, entry)
			packBytes += entry.Size
		}
		if readErr == io.EOF {
			return flush()
		}
	}
}

func s3CaptureDefinition(scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (string, error) {
	return captureledger.DigestCaptureDefinition(captureledger.CaptureDefinition{
		Source: captureledger.SourceIdentity{AccountID: scope.AccountID, Region: scope.Region}, Resource: captureledger.ResourceDescriptor{Service: ref.Service, Type: ref.Type, ID: ref.ID}, Mode: opts.Mode, Prefixes: opts.Prefixes,
		Limits: captureledger.Limits{MaxObjects: opts.Limits.MaxObjects, MaxItems: opts.Limits.MaxItems, MaxPages: opts.Limits.MaxPages, MaxObjectBytes: opts.Limits.MaxObjectBytes, MaxTotalBytes: opts.Limits.MaxTotalBytes}, Overwrite: opts.Overwrite, Gzip: opts.Gzip, PreserveProvisioned: opts.PreserveProvisioned, AllowPartialData: opts.AllowPartialData,
		PolicyIdentity: governance.IdentityOf(opts.Governance), DatasetFormat: "s3-tar-gzip-v1", DatasetVersion: 1, StructureVersion: model.CurrentSnapshotStructureVersion,
	})
}

func s3PackFreshness(entries []inventoryEntry) captureledger.FreshnessEvidence {
	components := make(map[string]string, len(entries))
	h := sha256.New()
	for _, entry := range entries {
		identity := sha256.Sum256([]byte(entry.Key))
		value := sha256.Sum256([]byte(entry.ETag + "\x00" + strconv.FormatInt(entry.Size, 10)))
		id, digest := hex.EncodeToString(identity[:]), hex.EncodeToString(value[:])
		components[id] = digest
		_, _ = io.WriteString(h, id+"\x00"+digest+"\n")
	}
	return captureledger.FreshnessEvidence{Kind: "s3_inventory_v1", Digest: hex.EncodeToString(h.Sum(nil)), Components: components}
}

func chunkArtifacts(chunk model.DataChunk) []captureledger.Artifact {
	result := []captureledger.Artifact{{Path: chunk.Data.Path, SHA256: chunk.Data.SHA256, Size: chunk.Data.Size, MediaType: chunk.Data.MediaType}}
	if chunk.Index != nil {
		result = append(result, captureledger.Artifact{Path: chunk.Index.Path, SHA256: chunk.Index.SHA256, Size: chunk.Index.Size, MediaType: chunk.Index.MediaType})
	}
	return result
}

func ledgerArtifactRef(artifact captureledger.Artifact) model.ArtifactRef {
	return model.ArtifactRef{Path: artifact.Path, SHA256: artifact.SHA256, Size: artifact.Size, MediaType: artifact.MediaType}
}

// estimateS3RemainingArtifactBytes uses only the durable inventory. Completed
// packs are excluded by starting at ProcessedOffset, and no object body is read
// before the disk-space decision. The allowance covers tar framing, compressed
// stream expansion, the object index, and checkpoint updates.
func estimateS3RemainingArtifactBytes(invPath string, processedOffset int64, compiled *captureGovernance) (int64, error) {
	f, err := os.Open(invPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err = f.Seek(processedOffset, io.SeekStart); err != nil {
		return 0, err
	}

	estimate := s3PreflightBaseOverhead
	var remainingObjects int64
	scanner := bufio.NewScanner(f)
	// Inventory records contain S3 keys and can legitimately exceed Scanner's
	// small default token limit.
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		remainingObjects++
		var entry inventoryEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return 0, err
		}
		outputSize := entry.Size
		if compiled != nil && compiled.body != nil {
			outputSize, err = governedS3OutputSize(*compiled.body)
			if err != nil {
				return 0, err
			}
		}
		// Deflate can expand incompressible input slightly. Add 12.5%, then a
		// deliberately generous per-object allowance for tar padding and index
		// metadata that is unavailable until GetObject.
		estimate = saturatedAdd(estimate, outputSize)
		estimate = saturatedAdd(estimate, outputSize/8)
		estimate = saturatedAdd(estimate, s3PreflightPerObjectOverhead)
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if remainingObjects == 0 {
		return 0, nil
	}
	return estimate, nil
}

func saturatedAdd(left, right int64) int64 {
	if right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func (a *Adapter) buildInventory(ctx context.Context, bucket string, prefixes []string, full bool, opts model.CaptureOptions, invPath, cpPath string, cp *s3Checkpoint, snap *model.Snapshot) (bool, error) {
	f, err := os.OpenFile(invPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err = f.Truncate(cp.InventoryBytes); err != nil {
		return false, err
	}
	if _, err = f.Seek(cp.InventoryBytes, io.SeekStart); err != nil {
		return false, err
	}
	w := bufio.NewWriter(f)
	for cp.Prefix < len(prefixes) {
		input := &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefixes[cp.Prefix])}
		if cp.Token != "" {
			input.ContinuationToken = aws.String(cp.Token)
		}
		o, e := a.client.ListObjectsV2(ctx, input)
		if e != nil {
			return false, awsconfig.Classify(e, "list objects in S3 bucket "+bucket, "")
		}
		stop := false
		for _, v := range o.Contents {
			key, etag, size := aws.ToString(v.Key), aws.ToString(v.ETag), aws.ToInt64(v.Size)
			if !full && int(cp.Records) >= opts.Limits.MaxObjects {
				snap.Findings = append(snap.Findings, truncated(bucket, "max_objects"))
				stop = true
				break
			}
			if !full && size > opts.Limits.MaxObjectBytes {
				snap.Findings = append(snap.Findings, truncated(key, "max_object_bytes"))
				continue
			}
			if size < 0 || (!full && cp.SourceBytes+size > opts.Limits.MaxTotalBytes) {
				snap.Findings = append(snap.Findings, truncated(bucket, "max_total_bytes"))
				stop = true
				break
			}
			line, _ := json.Marshal(inventoryEntry{key, etag, size})
			line = append(line, '\n')
			if _, err = w.Write(line); err != nil {
				return false, err
			}
			cp.InventoryBytes += int64(len(line))
			cp.Records++
			cp.SourceBytes += size
		}
		if err = w.Flush(); err != nil {
			return false, err
		}
		if err = f.Sync(); err != nil {
			return false, err
		}
		if stop {
			cp.InventoryComplete = true
			if err := saveS3Checkpoint(cpPath, *cp); err != nil {
				return false, err
			}
			return true, nil
		}
		cp.Token = aws.ToString(o.NextContinuationToken)
		if cp.Token == "" {
			cp.Prefix++
		}
		if cp.Prefix == len(prefixes) {
			cp.InventoryComplete = true
		}
		if err := saveS3Checkpoint(cpPath, *cp); err != nil {
			return false, err
		}
		if opts.Progress != nil {
			opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "inventory", Service: "s3", Resource: bucket, CompletedRecords: cp.Records, CompletedBytes: cp.SourceBytes, TotalPrecision: "unknown"})
		}
	}
	return false, nil
}

func (a *Adapter) capturePack(ctx context.Context, bucket string, number int, entries []inventoryEntry, opts model.CaptureOptions, compiled *captureGovernance, snap *model.Snapshot) (model.DataChunk, error) {
	sum := sha256.Sum256([]byte(bucket))
	base := "bundle/data/s3/" + hex.EncodeToString(sum[:16])
	packRel := fmt.Sprintf("%s/pack-%06d.tar.gz", base, number)
	indexRel := fmt.Sprintf("%s/pack-%06d.index.ndjson.gz", base, number)
	_ = os.Remove(filepath.Join(opts.ArtifactDirectory, filepath.FromSlash(packRel)))
	_ = os.Remove(filepath.Join(opts.ArtifactDirectory, filepath.FromSlash(indexRel)))
	var objects []Object
	pack, err := writeS3Artifact(ctx, opts.ArtifactDirectory, packRel, "application/gzip", func(w io.Writer) error {
		gz := gzip.NewWriter(w)
		gz.Header.ModTime = time.Unix(0, 0)
		tw := tar.NewWriter(gz)
		for _, entry := range entries {
			o, e := a.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(entry.Key), IfMatch: emptyNil(entry.ETag), ChecksumMode: types.ChecksumModeEnabled})
			if e != nil {
				return awsconfig.Classify(e, "download S3 object "+entry.Key, "")
			}
			id := sha256.Sum256([]byte(bucket + "\x00" + entry.Key))
			tarName := hex.EncodeToString(id[:]) + ".bin"
			bodyRule, governed, e := compiled.bodyRule(o)
			if e != nil {
				o.Body.Close()
				return e
			}
			bodySize := entry.Size
			if governed {
				bodySize, e = governedS3OutputSize(bodyRule)
				if e != nil {
					o.Body.Close()
					return e
				}
			}
			if e = tw.WriteHeader(&tar.Header{Name: tarName, Mode: 0o600, Size: bodySize, ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg}); e != nil {
				o.Body.Close()
				return e
			}
			h := sha256.New()
			detector := bundle.NewCredentialDetector()
			var n int64
			output := &countingWriter{w: io.MultiWriter(tw, h, detector), n: &n}
			if governed {
				limited := &io.LimitedReader{R: &contextReader{ctx, o.Body}, N: entry.Size}
				_, e = compiled.engine.ApplyReader(bodyRule, limited, output)
				if e == nil && (bodyRule.Action == governance.ActionHash || bodyRule.Action == governance.ActionPseudonymize) && limited.N != 0 {
					e = io.ErrUnexpectedEOF
				}
			} else {
				_, e = io.CopyN(output, &contextReader{ctx, o.Body}, entry.Size)
			}
			closeErr := o.Body.Close()
			if e == nil {
				e = closeErr
			}
			if e != nil {
				return e
			}
			if e = detector.Err(); e != nil {
				return fmt.Errorf("%w in S3 object %q", e, entry.Key)
			}
			if governed {
				opts.GovernanceAudit.Record(bodyRule.ID)
			}
			metadata, e := compiled.governMetadata(o.Metadata, opts.GovernanceAudit)
			if e != nil {
				return e
			}
			obj := Object{Key: entry.Key, Path: tarName, Size: n, SHA256: hex.EncodeToString(h.Sum(nil)), ETag: entry.ETag, ContentType: aws.ToString(o.ContentType), ContentEncoding: aws.ToString(o.ContentEncoding), CacheControl: aws.ToString(o.CacheControl), Metadata: metadata, Checksums: map[string]string{}, Overwrite: s3CaptureOverwrite(opts.Overwrite)}
			if !governed {
				for k, v := range map[string]*string{"crc32": o.ChecksumCRC32, "crc32c": o.ChecksumCRC32C, "sha1": o.ChecksumSHA1, "sha256": o.ChecksumSHA256} {
					if v != nil {
						obj.Checksums[k] = *v
					}
				}
			}
			if len(obj.Checksums) == 0 {
				obj.Checksums = nil
			}
			if tags, e := a.client.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(entry.Key)}); e != nil {
				if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
					return e
				}
				snap.Findings = append(snap.Findings, optionalFinding(entry.Key, "S3_OBJECT_TAGS_UNAVAILABLE", "tags", e))
			} else {
				for _, t := range tags.TagSet {
					obj.Tags = append(obj.Tags, Tag{aws.ToString(t.Key), aws.ToString(t.Value)})
				}
				sortTags(obj.Tags)
			}
			objects = append(objects, obj)
		}
		if err := tw.Close(); err != nil {
			return err
		}
		return gz.Close()
	})
	if err != nil {
		return model.DataChunk{}, err
	}
	index, err := writeS3Artifact(ctx, opts.ArtifactDirectory, indexRel, "application/gzip", func(w io.Writer) error {
		gz := gzip.NewWriter(w)
		gz.Header.ModTime = time.Unix(0, 0)
		enc := json.NewEncoder(gz)
		enc.SetEscapeHTML(false)
		for _, obj := range objects {
			if err := enc.Encode(obj); err != nil {
				return err
			}
		}
		return gz.Close()
	})
	if err != nil {
		return model.DataChunk{}, err
	}
	return model.DataChunk{Data: pack, Index: &index, Records: int64(len(entries)), SourceBytes: inventorySize(entries)}, nil
}

func governedS3BodyRule(bucket string, object *awss3.GetObjectOutput, policy *governance.EffectivePolicy) (governance.Rule, bool, error) {
	return newCaptureGovernance(bucket, policy).bodyRule(object)
}

func (compiled *captureGovernance) bodyRule(object *awss3.GetObjectOutput) (governance.Rule, bool, error) {
	if compiled == nil || compiled.body == nil {
		return governance.Rule{}, false, nil
	}
	rule := *compiled.body
	contentType := aws.ToString(object.ContentType)
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !slices.Contains(rule.ContentTypes, strings.ToLower(mediaType)) {
		return governance.Rule{}, false, fmt.Errorf("S3 object content type %q is not allowed by governance rule %q", contentType, rule.ID)
	}
	return rule, true, nil
}

func governedS3OutputSize(rule governance.Rule) (int64, error) {
	switch rule.Action {
	case governance.ActionOmit:
		return 0, nil
	case governance.ActionReplace:
		return int64(len(rule.Replacement)), nil
	case governance.ActionHash:
		return int64(len(governance.HashAlgorithm) + 1 + hex.EncodedLen(sha256.Size)), nil
	case governance.ActionPseudonymize:
		return int64(len(governance.PseudonymAlgorithm) + 1 + hex.EncodedLen(sha256.Size)), nil
	default:
		return 0, governance.ErrInvalidTransformation
	}
}

func governedS3Metadata(bucket string, source map[string]string, policy *governance.EffectivePolicy, audit *governance.Audit) (map[string]string, error) {
	return newCaptureGovernance(bucket, policy).governMetadata(source, audit)
}

func (compiled *captureGovernance) governMetadata(source map[string]string, audit *governance.Audit) (map[string]string, error) {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	if compiled == nil || compiled.engine == nil {
		return result, nil
	}
	for key, value := range result {
		for _, rule := range compiled.metadata[strings.ToLower(key)] {
			transformed, err := compiled.engine.Apply(rule, []byte(value))
			if err != nil {
				return nil, fmt.Errorf("apply S3 governance rule %q: %w", rule.ID, err)
			}
			if transformed.Omit {
				delete(result, key)
			} else {
				result[key] = string(transformed.Value)
			}
			if audit != nil {
				audit.Record(rule.ID)
			}
			if transformed.Omit {
				break
			}
			value = string(transformed.Value)
		}
	}
	return result, nil
}

func s3BodyRule(bucket string, policy *governance.EffectivePolicy) (governance.Rule, bool) {
	if policy != nil {
		for _, rule := range policy.Rules {
			if rule.Service == governance.ServiceS3 && rule.Resource == bucket && rule.Target.Kind == governance.TargetS3TextBody {
				return rule, true
			}
		}
	}
	return governance.Rule{}, false
}

func writeS3Artifact(ctx context.Context, root, rel, media string, write func(io.Writer) error) (model.ArtifactRef, error) {
	target := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return model.ArtifactRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".partial-")
	if err != nil {
		return model.ArtifactRef{}, err
	}
	name := tmp.Name()
	defer os.Remove(name)
	h := sha256.New()
	var n int64
	cw := &countingWriter{w: io.MultiWriter(tmp, h), n: &n}
	err = write(cw)
	if syncErr := tmp.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return model.ArtifactRef{}, err
	}
	if err = ctx.Err(); err != nil {
		return model.ArtifactRef{}, err
	}
	if err = os.Rename(name, target); err != nil {
		return model.ArtifactRef{}, err
	}
	return model.ArtifactRef{Path: rel, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n, MediaType: media}, nil
}

type countingWriter struct {
	w io.Writer
	n *int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, e := c.w.Write(p)
	*c.n += int64(n)
	return n, e
}
func inventorySize(v []inventoryEntry) int64 {
	var n int64
	for _, x := range v {
		n += x.Size
	}
	return n
}
func chunkBytes(v []model.DataChunk) int64 {
	var n int64
	for _, x := range v {
		n += x.SourceBytes
	}
	return n
}
func loadS3Checkpoint(path, bucket string, opts model.CaptureOptions, prefixes []string) (s3Checkpoint, bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return newS3Checkpoint(bucket, opts, prefixes), false, nil
	}
	if err != nil {
		return s3Checkpoint{}, false, err
	}
	var cp s3Checkpoint
	if err = json.Unmarshal(b, &cp); err != nil {
		return cp, false, err
	}
	if cp.Version != s3CheckpointVersion || cp.Bucket != bucket || !s3CaptureIdentityMatches(cp, opts, prefixes) {
		return cp, false, fmt.Errorf("incompatible S3 capture checkpoint; restart the capture (--restart) or use a different --work-dir")
	}
	return cp, true, nil
}

func newS3Checkpoint(bucket string, opts model.CaptureOptions, prefixes []string) s3Checkpoint {
	return s3Checkpoint{
		Version:        s3CheckpointVersion,
		Bucket:         bucket,
		Mode:           s3CaptureMode(opts.Mode),
		Prefixes:       append([]string(nil), prefixes...),
		MaxObjects:     opts.Limits.MaxObjects,
		MaxObjectBytes: opts.Limits.MaxObjectBytes,
		MaxTotalBytes:  opts.Limits.MaxTotalBytes,
		Overwrite:      s3CaptureOverwrite(opts.Overwrite),
		PolicyIdentity: governance.IdentityOf(opts.Governance),
	}
}

// s3CaptureIdentityMatches requires identical capture options on resume: the
// inventory scope (prefixes), bounded limits, mode, and overwrite policy are all
// baked into the durable inventory, packs, and indexes, so a changed definition
// must start from a fresh checkpoint rather than silently mixing scopes.
func s3CaptureIdentityMatches(cp s3Checkpoint, opts model.CaptureOptions, prefixes []string) bool {
	return cp.Mode == s3CaptureMode(opts.Mode) &&
		slices.Equal(cp.Prefixes, prefixes) &&
		cp.MaxObjects == opts.Limits.MaxObjects &&
		cp.MaxObjectBytes == opts.Limits.MaxObjectBytes &&
		cp.MaxTotalBytes == opts.Limits.MaxTotalBytes &&
		cp.Overwrite == s3CaptureOverwrite(opts.Overwrite) &&
		cp.PolicyIdentity == governance.IdentityOf(opts.Governance)
}

func s3CaptureMode(mode string) string {
	if mode == "" {
		return "bounded"
	}
	return mode
}

func s3CaptureOverwrite(policy string) string {
	if policy == "" {
		return "if-different"
	}
	return policy
}

func saveS3Checkpoint(path string, cp s3Checkpoint) error {
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return storage.WriteFileSync(path, b)
}

func compactPrefixes(prefixes []string) []string {
	out := prefixes[:0]
	for _, prefix := range prefixes {
		if len(out) == 0 || !strings.HasPrefix(prefix, out[len(out)-1]) {
			out = append(out, prefix)
		}
	}
	return out
}

func writeObject(ctx context.Context, bucket, key, etag string, o *awss3.GetObjectOutput, opts model.CaptureOptions) (Object, model.ArtifactRef, error) {
	id := sha256.Sum256([]byte(bucket + "\x00" + key))
	rel := filepath.ToSlash(filepath.Join("bundle", "data", "s3", hex.EncodeToString(id[:])+".bin"))
	target := filepath.Join(opts.ArtifactDirectory, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return Object{}, model.ArtifactRef{}, err
	}
	tmp, e := os.CreateTemp(filepath.Dir(target), ".partial-")
	if e != nil {
		return Object{}, model.ArtifactRef{}, e
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	_ = tmp.Chmod(0600)
	h := sha256.New()
	limited := io.LimitReader(o.Body, opts.Limits.MaxObjectBytes+1)
	n, e := io.Copy(io.MultiWriter(tmp, h), &contextReader{ctx, limited})
	if ce := tmp.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return Object{}, model.ArtifactRef{}, e
	}
	if n > opts.Limits.MaxObjectBytes {
		return Object{}, model.ArtifactRef{}, fmt.Errorf("S3 object %q exceeded max object bytes while streaming", key)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return Object{}, model.ArtifactRef{}, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	overwrite := opts.Overwrite
	if overwrite == "" {
		overwrite = "if-different"
	}
	obj := Object{Key: key, Path: rel, Size: n, SHA256: digest, ETag: etag, ContentType: aws.ToString(o.ContentType), ContentEncoding: aws.ToString(o.ContentEncoding), CacheControl: aws.ToString(o.CacheControl), Metadata: o.Metadata, Checksums: map[string]string{}, Overwrite: overwrite}
	for k, v := range map[string]*string{"crc32": o.ChecksumCRC32, "crc32c": o.ChecksumCRC32C, "sha1": o.ChecksumSHA1, "sha256": o.ChecksumSHA256} {
		if v != nil {
			obj.Checksums[k] = *v
		}
	}
	if len(obj.Checksums) == 0 {
		obj.Checksums = nil
	}
	return obj, model.ArtifactRef{Path: rel, SHA256: digest, Size: n, MediaType: "application/octet-stream"}, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *contextReader) Read(p []byte) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	default:
		return c.r.Read(p)
	}
}
func emptyNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
func sortTags(v []Tag) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].Key == v[j].Key {
			return v[i].Value < v[j].Value
		}
		return v[i].Key < v[j].Key
	})
}
func canonicalJSON(value string) string {
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return value
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return value
	}
	return string(encoded)
}
