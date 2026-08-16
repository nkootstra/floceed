package dynamodb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

const dynamoChunkBytes int64 = 64 << 20
const mergeFanIn = 64
const governedCheckpointItemInterval = 100_000
const governedCheckpointByteInterval int64 = 128 << 20

type ArtifactWriter interface {
	WriteArtifact(context.Context, string, func(io.Writer) error) (model.ArtifactRef, error)
}

type directoryWriter struct{ root string }
type byteCounter int64

func (c *byteCounter) Write(p []byte) (int, error) { *c += byteCounter(len(p)); return len(p), nil }

func (w directoryWriter) WriteArtifact(ctx context.Context, name string, write func(io.Writer) error) (model.ArtifactRef, error) {
	clean := filepath.Clean(filepath.FromSlash(name))
	if w.root == "" || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return model.ArtifactRef{}, fmt.Errorf("unsafe artifact path %q", name)
	}
	if err := ctx.Err(); err != nil {
		return model.ArtifactRef{}, err
	}
	destination := filepath.Join(w.root, clean)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return model.ArtifactRef{}, err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return model.ArtifactRef{}, err
	}
	hash := sha256.New()
	var size byteCounter
	err = write(io.MultiWriter(file, hash, &size))
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(destination)
		return model.ArtifactRef{}, err
	}
	return model.ArtifactRef{Path: filepath.ToSlash(name), SHA256: hex.EncodeToString(hash.Sum(nil)), Size: int64(size)}, nil
}

type DataResult struct {
	Artifact         model.ArtifactRef
	Dataset          model.Dataset
	Items, Pages     int
	ConsumedCapacity float64
	Truncated        bool
}

// captureCheckpointVersion 4 records the capture definition, bounded-run
// truncation, progress, and the governance identity. A checkpoint
// can therefore only be resumed by a run with identical capture options while
// preserving the final result classification.
const captureCheckpointVersion = 5

type captureCheckpoint struct {
	Version            int                               `json:"version"`
	Table              string                            `json:"table"`
	Mode               string                            `json:"mode"`
	MaxItems           int                               `json:"max_items,omitempty"`
	MaxPages           int                               `json:"max_pages,omitempty"`
	GovernanceIdentity string                            `json:"governance_identity,omitempty"`
	LastKey            json.RawMessage                   `json:"last_key,omitempty"`
	ProtectedLastKey   []byte                            `json:"protected_last_key,omitempty"`
	ScanComplete       bool                              `json:"scan_complete,omitempty"`
	Runs               []checkpointRun                   `json:"runs,omitempty"`
	CohortSelection    []governance.CohortSelectionState `json:"cohort_selection,omitempty"`
	Items              int                               `json:"items,omitempty"`
	Pages              int                               `json:"pages,omitempty"`
	Truncated          bool                              `json:"truncated,omitempty"`
	SourceBytes        int64                             `json:"source_bytes,omitempty"`
	ConsumedCapacity   float64                           `json:"consumed_capacity,omitempty"`
	GovernanceCounts   map[string]int                    `json:"governance_rule_counts,omitempty"`
	ProtectedState     *protectedStateRef                `json:"protected_state,omitempty"`
	ScannedItems       int                               `json:"scanned_items,omitempty"`
}
type protectedStateRef struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type governedResumeState struct {
	LastKey          json.RawMessage `json:"last_key,omitempty"`
	ScanComplete     bool            `json:"scan_complete"`
	Runs             []checkpointRun `json:"runs,omitempty"`
	Items            int             `json:"items"`
	Pages            int             `json:"pages"`
	Truncated        bool            `json:"truncated"`
	SourceBytes      int64           `json:"source_bytes"`
	ConsumedCapacity float64         `json:"consumed_capacity"`
	GovernanceCounts map[string]int  `json:"governance_rule_counts,omitempty"`
	SelectionCount   int             `json:"selection_count"`
	ScannedItems     int             `json:"scanned_items"`
}
type checkpointRun struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// CaptureData retains the bounded v1 call surface while using the streaming engine.
func (a *Adapter) CaptureData(ctx context.Context, table string, limits model.DataLimits, gzipEnabled bool, sink ArtifactWriter) (DataResult, error) {
	return a.captureData(ctx, table, model.CaptureOptions{Mode: "bounded", Limits: limits, Gzip: gzipEnabled}, sink)
}

func (a *Adapter) captureData(ctx context.Context, table string, opts model.CaptureOptions, sink ArtifactWriter) (DataResult, error) {
	full := opts.Mode == "full"
	if opts.Governance != nil && len(opts.Governance.Secret()) < 32 {
		return DataResult{}, fmt.Errorf("governed DynamoDB capture requires FLOCEED_GOVERNANCE_SECRET for protected checkpoints")
	}
	var cohort *governance.Cohort
	if opts.Governance != nil {
		for i := range opts.Governance.Cohorts {
			if opts.Governance.Cohorts[i].Resource == table {
				cohort = &opts.Governance.Cohorts[i]
				break
			}
		}
	}
	if !full && (opts.Limits.MaxItems <= 0 || opts.Limits.MaxPages <= 0) {
		if cohort == nil {
			return DataResult{}, fmt.Errorf("positive DynamoDB item and page limits are required")
		}
	}
	work := opts.CheckpointDirectory
	removeWork := false
	if work == "" {
		var err error
		work, err = os.MkdirTemp("", "floceed-ddb-")
		if err != nil {
			return DataResult{}, err
		}
		removeWork = true
	}
	if removeWork {
		defer os.RemoveAll(work)
	}
	if err := os.MkdirAll(work, 0o700); err != nil {
		return DataResult{}, err
	}
	cpPath := filepath.Join(work, "checkpoint.json")
	cp, resumed, err := loadCheckpoint(cpPath, table, opts)
	if err != nil {
		return DataResult{}, err
	}
	var restoredSelection []governance.CohortSelectionState
	if opts.Governance != nil && resumed {
		var state governedResumeState
		state, restoredSelection, err = loadGovernedState(work, cp, opts)
		if err != nil {
			return DataResult{}, fmt.Errorf("corrupt DynamoDB capture checkpoint; restart the capture")
		}
		applyGovernedState(&cp, state)
	}
	opts.GovernanceAudit.RestoreRuleCounts(cp.GovernanceCounts)
	var key map[string]types.AttributeValue
	resumeKey := []byte(cp.LastKey)
	if len(resumeKey) != 0 {
		key, err = decodeKey(resumeKey)
		if err != nil {
			return DataResult{}, fmt.Errorf("decode DynamoDB resume key: %w", err)
		}
	}
	r := DataResult{Items: cp.Items, Pages: cp.Pages, ConsumedCapacity: cp.ConsumedCapacity, Truncated: cp.Truncated}
	diskChecked := false
	checkDisk := func() error {
		if !full || diskChecked {
			return nil
		}
		estimate := opts.EstimatedBytes
		if opts.EstimatedRecords > 0 && cp.Items > 0 {
			sampled := scaleEstimate(cp.SourceBytes, opts.EstimatedRecords, int64(cp.Items))
			if sampled > estimate {
				estimate = sampled
			}
		}
		if estimate == 0 {
			return nil
		}
		diskChecked = true
		return storage.RequireAvailable(opts.ArtifactDirectory, estimate, 2)
	}
	if cp.Items > 0 {
		if err := checkDisk(); err != nil {
			return r, err
		}
	}
	emit := func(phase, precision string) {
		if opts.Progress != nil {
			total := opts.EstimatedRecords
			completed := int64(r.Items)
			if cohort != nil {
				// Eligible counts are privacy-sensitive. The public progress surface
				// reports only the configured retention boundary.
				completed = min(completed, int64(cohort.Limit))
				total = int64(cohort.Limit)
			}
			if precision == "exact" {
				total = completed
			}
			opts.Progress(model.ProgressEvent{SchemaVersion: 1, Event: "progress", Operation: "pull", Phase: phase, Service: "dynamodb", Resource: table, CompletedRecords: completed, TotalRecords: total, TotalPrecision: precision, Resumed: resumed})
		}
	}
	emit("capture", "estimated")
	detector := bundle.NewCredentialDetector()
	var rules []governance.Rule
	var compiledRules []compiledDynamoRule
	var engine *governance.Engine
	if opts.Governance != nil {
		for _, rule := range opts.Governance.Rules {
			if rule.Service == governance.ServiceDynamoDB && rule.Resource == table {
				rules = append(rules, rule)
			}
		}
		engine = governance.NewEngine(opts.Governance.Profile, opts.Governance.Secret())
		compiledRules = compileDynamoRules(rules)
	}
	var ranker *governance.CohortRanker
	var selection *governance.CohortSelection
	if cohort != nil {
		ranker, err = governance.NewCohortRanker(opts.Governance.Profile, opts.Governance.Secret(), *cohort)
		if err != nil {
			return r, err
		}
		selection = ranker.NewSelection(nil)
		for _, candidate := range restoredSelection {
			if err = selection.RestoreOwned(candidate.Rank, candidate.Value); err != nil {
				return r, fmt.Errorf("corrupt DynamoDB capture checkpoint; restart the capture")
			}
		}
	}
	lastCheckpointItems, lastCheckpointBytes := cp.ScannedItems, cp.SourceBytes
	for !cp.ScanComplete && (full || cohort != nil || (r.Pages < opts.Limits.MaxPages && r.Items < opts.Limits.MaxItems)) {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		input := &awsddb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: key, ConsistentRead: aws.Bool(false), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal}
		if !full && cohort == nil {
			input.Limit = aws.Int32(int32(opts.Limits.MaxItems - r.Items))
		}
		o, scanErr := a.client.Scan(ctx, input)
		if scanErr != nil {
			return r, scanErr
		}
		r.Pages++
		if o.ConsumedCapacity != nil {
			r.ConsumedCapacity += aws.ToFloat64(o.ConsumedCapacity.CapacityUnits)
		}
		rows := make([][]byte, 0, len(o.Items))
		for _, item := range o.Items {
			cp.ScannedItems++
			if !full && cohort == nil && r.Items >= opts.Limits.MaxItems {
				r.Truncated = true
				break
			}
			var keyValues [][]byte
			if cohort != nil {
				var eligible bool
				keyValues, eligible, err = cohortKeyValues(item, *cohort)
				if err != nil {
					return r, err
				}
				if !eligible {
					continue
				}
			}
			if len(rules) != 0 {
				var e error
				item, e = governItemCompiled(item, compiledRules, engine, opts.GovernanceAudit)
				if e != nil {
					return r, e
				}
			}
			b, e := CanonicalItem(item)
			if e != nil {
				return r, e
			}
			_, _ = detector.Write(b)
			if e = detector.Err(); e != nil {
				return r, fmt.Errorf("%w in DynamoDB table %q", e, table)
			}
			if cohort != nil {
				if e = selection.OfferChecked(keyValues, b); e != nil {
					return r, fmt.Errorf("retain DynamoDB cohort: %w", e)
				}
			} else {
				rows = append(rows, b)
			}
			r.Items++
			cp.SourceBytes += int64(len(b) + 1)
		}
		if cohort == nil {
			sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i], rows[j]) < 0 })
			run := filepath.Join(work, fmt.Sprintf("page-%09d.run", r.Pages))
			if err := writeRun(run, rows); err != nil {
				return r, err
			}
			runRef, e := sumRun(run)
			if e != nil {
				return r, e
			}
			cp.Runs = append(cp.Runs, runRef)
		}
		cp.Items, cp.Pages, cp.ConsumedCapacity = r.Items, r.Pages, r.ConsumedCapacity
		key = o.LastEvaluatedKey
		encodedKey, encodeErr := encodeKey(key)
		err = encodeErr
		if err != nil {
			return r, err
		}
		cp.LastKey = encodedKey
		cp.ProtectedLastKey = nil
		if !full && cohort == nil && len(key) > 0 && (r.Pages == opts.Limits.MaxPages || r.Items == opts.Limits.MaxItems) {
			r.Truncated = true
		}
		cp.ScanComplete = len(key) == 0 || r.Truncated
		cp.Truncated = r.Truncated
		cp.GovernanceCounts = opts.GovernanceAudit.RuleCounts()
		shouldCheckpoint := opts.Governance == nil || r.Pages == 1 || cp.ScannedItems-lastCheckpointItems >= governedCheckpointItemInterval || cp.SourceBytes-lastCheckpointBytes >= governedCheckpointByteInterval || cp.ScanComplete
		if shouldCheckpoint {
			if opts.Governance != nil {
				if err := saveGovernedCheckpoint(work, cpPath, &cp, selection, opts); err != nil {
					return r, err
				}
				lastCheckpointItems, lastCheckpointBytes = cp.ScannedItems, cp.SourceBytes
			} else if err := saveCheckpoint(cpPath, cp); err != nil {
				return r, err
			}
		}
		if err := checkDisk(); err != nil {
			return r, err
		}
		emit("capture", "estimated")
		if len(key) == 0 {
			break
		}
		if r.Truncated {
			break
		}
	}
	emit("finalize", "exact")
	var merged string
	if cohort != nil {
		merged = filepath.Join(work, "cohort-selected.run")
		if err = writeRun(merged, selection.Values()); err != nil {
			return r, err
		}
		r.Truncated = r.Items > cohort.Limit
	} else {
		runPaths := make([]string, len(cp.Runs))
		for i, run := range cp.Runs {
			runPaths[i] = run.Path
		}
		merged, err = mergeRuns(ctx, work, runPaths)
		if err != nil {
			return r, err
		}
	}
	dataset, err := writeDynamoChunks(ctx, table, merged, opts.Gzip, sink)
	if err != nil {
		return r, err
	}
	dataset.Resumed = resumed
	r.Dataset = dataset
	if cohort != nil {
		retained := min(r.Items, cohort.Limit)
		opts.GovernanceAudit.RecordCohort(governanceResourceIdentity(opts.Governance, table), r.Items, retained, r.Truncated)
	}
	if len(dataset.Chunks) > 0 {
		r.Artifact = dataset.Chunks[0].Data
	}
	return r, nil
}

func governanceResourceIdentity(policy *governance.EffectivePolicy, resource string) string {
	identity := governance.IdentityOf(policy)
	if identity == "" {
		return ""
	}
	digest := sha256.Sum256([]byte("floceed/governance/resource/v1\x00" + identity + "\x00" + resource))
	return hex.EncodeToString(digest[:])
}

func writeRun(path string, rows [][]byte) error {
	// O_TRUNC (not O_EXCL): a crash between writing a page run and saving the
	// checkpoint can leave an orphaned run file that is not referenced by the
	// checkpoint; the next resume reuses the same page number and must be able
	// to replace that orphan rather than fail permanently.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, row := range rows {
		if _, err = w.Write(row); err == nil {
			err = w.WriteByte('\n')
		}
		if err != nil {
			break
		}
	}
	if flush := w.Flush(); err == nil {
		err = flush
	}
	// Make the run durable before the checkpoint references it, so a power
	// loss cannot produce a referenced-but-torn run.
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
	}
	return err
}

type mergeLine struct {
	line  []byte
	index int
}
type lineHeap []mergeLine

func (h lineHeap) Len() int           { return len(h) }
func (h lineHeap) Less(i, j int) bool { return bytes.Compare(h[i].line, h[j].line) < 0 }
func (h lineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *lineHeap) Push(x any)        { *h = append(*h, x.(mergeLine)) }
func (h *lineHeap) Pop() any          { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

func mergeRuns(ctx context.Context, work string, runs []string) (string, error) {
	if len(runs) == 0 {
		empty := filepath.Join(work, "empty.run")
		return empty, os.WriteFile(empty, nil, 0o600)
	}
	pass := 0
	for len(runs) > 1 {
		var next []string
		for start := 0; start < len(runs); start += mergeFanIn {
			end := min(start+mergeFanIn, len(runs))
			out := filepath.Join(work, fmt.Sprintf("merge-%03d-%06d.run", pass, len(next)))
			if err := mergeGroup(ctx, runs[start:end], out); err != nil {
				return "", err
			}
			next = append(next, out)
		}
		if pass > 0 {
			for _, p := range runs {
				_ = os.Remove(p)
			}
		}
		runs = next
		pass++
	}
	return runs[0], nil
}

func mergeGroup(ctx context.Context, paths []string, output string) error {
	files := make([]*os.File, len(paths))
	scans := make([]*bufio.Scanner, len(paths))
	h := lineHeap{}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for i, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		files[i] = f
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 64<<10), 2<<20)
		scans[i] = s
		if s.Scan() {
			heap.Push(&h, mergeLine{append([]byte(nil), s.Bytes()...), i})
		} else if err := s.Err(); err != nil {
			return err
		}
	}
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			out.Close()
			return err
		}
		x := heap.Pop(&h).(mergeLine)
		if _, err = w.Write(x.line); err == nil {
			err = w.WriteByte('\n')
		}
		if err != nil {
			out.Close()
			return err
		}
		s := scans[x.index]
		if s.Scan() {
			heap.Push(&h, mergeLine{append([]byte(nil), s.Bytes()...), x.index})
		} else if err := s.Err(); err != nil {
			out.Close()
			return err
		}
	}
	if err = w.Flush(); err == nil {
		err = out.Close()
	}
	return err
}

func writeDynamoChunks(ctx context.Context, table, merged string, gzipEnabled bool, sink ArtifactWriter) (model.Dataset, error) {
	f, err := os.Open(merged)
	if err != nil {
		return model.Dataset{}, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	sum := sha256.Sum256([]byte(table))
	base := "bundle/data/dynamodb/" + hex.EncodeToString(sum[:16])
	if writer, ok := sink.(directoryWriter); ok {
		_ = os.RemoveAll(filepath.Join(writer.root, filepath.FromSlash(base)))
	}
	format := "dynamodb-ndjson-v1"
	ext := ".ndjson"
	media := "application/x-ndjson"
	if gzipEnabled {
		format = "dynamodb-ndjson-gzip-v1"
		ext += ".gz"
		media = "application/gzip"
	}
	d := model.Dataset{Format: format, Records: 0, Consistency: "best_effort"}
	pending, readErr := reader.ReadBytes('\n')
	if readErr == io.EOF && len(pending) == 0 {
		return d, nil
	}
	if readErr != nil && readErr != io.EOF {
		return d, readErr
	}
	for i := 1; len(pending) > 0; i++ {
		var records, rawBytes int64
		rel := fmt.Sprintf("%s/part-%06d%s", base, i, ext)
		art, e := sink.WriteArtifact(ctx, rel, func(w io.Writer) error {
			var dst io.Writer = w
			var gz *gzip.Writer
			if gzipEnabled {
				gz = gzip.NewWriter(w)
				gz.Header.ModTime = time.Unix(0, 0)
				gz.Header.Name = ""
				dst = gz
			}
			for len(pending) > 0 {
				if records > 0 && rawBytes+int64(len(pending)) > dynamoChunkBytes {
					break
				}
				if _, e := dst.Write(pending); e != nil {
					return e
				}
				records++
				rawBytes += int64(len(pending))
				pending, readErr = reader.ReadBytes('\n')
				if readErr != nil && readErr != io.EOF {
					return readErr
				}
				if readErr == io.EOF && len(pending) == 0 {
					break
				}
			}
			if gz != nil {
				return gz.Close()
			}
			return nil
		})
		if e != nil {
			return d, e
		}
		art.MediaType = media
		d.Records += records
		d.SourceBytes += rawBytes
		d.Chunks = append(d.Chunks, model.DataChunk{Data: art, Records: records, SourceBytes: rawBytes})
	}
	return d, nil
}

func loadCheckpoint(path, table string, opts model.CaptureOptions) (captureCheckpoint, bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return newCheckpoint(table, opts), false, nil
	}
	if err != nil {
		return captureCheckpoint{}, false, err
	}
	var cp captureCheckpoint
	if err = json.Unmarshal(b, &cp); err != nil {
		return cp, false, err
	}
	if cp.Version != captureCheckpointVersion || cp.Table != table || !captureIdentityMatches(cp, opts) {
		return cp, false, fmt.Errorf("incompatible DynamoDB capture checkpoint; restart the capture (--restart) or use a different --work-dir")
	}
	for _, run := range cp.Runs {
		if info, e := os.Stat(run.Path); e != nil || !info.Mode().IsRegular() {
			return cp, false, fmt.Errorf("corrupt DynamoDB capture checkpoint; restart the capture")
		}
		got, e := sumRun(run.Path)
		if e != nil || got.SHA256 != run.SHA256 || got.Size != run.Size {
			return cp, false, fmt.Errorf("corrupt DynamoDB capture checkpoint; restart the capture")
		}
	}
	if opts.Governance != nil {
		if len(cp.LastKey) != 0 || len(cp.ProtectedLastKey) != 0 || cp.ScanComplete || len(cp.Runs) != 0 || len(cp.CohortSelection) != 0 || cp.Items != 0 || cp.Pages != 0 || cp.Truncated || cp.SourceBytes != 0 || cp.ConsumedCapacity != 0 || len(cp.GovernanceCounts) != 0 || cp.ScannedItems != 0 || cp.ProtectedState == nil {
			return cp, false, fmt.Errorf("corrupt DynamoDB capture checkpoint; restart the capture")
		}
	}
	return cp, true, nil
}

const governedStateMagic = "FLCGST1\n"

func saveGovernedCheckpoint(work, checkpointPath string, cp *captureCheckpoint, selection *governance.CohortSelection, opts model.CaptureOptions) error {
	state := governedResumeState{LastKey: cp.LastKey, ScanComplete: cp.ScanComplete, Runs: cp.Runs, Items: cp.Items, Pages: cp.Pages, Truncated: cp.Truncated, SourceBytes: cp.SourceBytes, ConsumedCapacity: cp.ConsumedCapacity, GovernanceCounts: cp.GovernanceCounts, ScannedItems: cp.ScannedItems}
	if selection != nil {
		state.SelectionCount = selection.Len()
	}
	tmp, err := os.CreateTemp(work, ".governed-state-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err = tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	h := sha256.New()
	var written byteCounter
	count := &written
	w := io.MultiWriter(tmp, h, count)
	if _, err = io.WriteString(w, governedStateMagic); err == nil {
		var meta []byte
		meta, err = json.Marshal(state)
		if err == nil {
			err = writeProtectedRecord(w, opts.Governance, checkpointProtectionIdentity(*cp), 1, 0, meta)
		}
	}
	ordinal := uint64(1)
	if err == nil && selection != nil {
		err = selection.Visit(func(rank, value []byte) error {
			payload := make([]byte, 8+len(rank)+len(value))
			binary.BigEndian.PutUint32(payload[:4], uint32(len(rank)))
			copy(payload[4:], rank)
			binary.BigEndian.PutUint32(payload[4+len(rank):8+len(rank)], uint32(len(value)))
			copy(payload[8+len(rank):], value)
			e := writeProtectedRecord(w, opts.Governance, checkpointProtectionIdentity(*cp), 2, ordinal, payload)
			ordinal++
			return e
		})
	}
	if err == nil {
		err = writeProtectedRecord(w, opts.Governance, checkpointProtectionIdentity(*cp), 255, ordinal, nil)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	name := "governed-state-" + digest + ".bin"
	path := filepath.Join(work, name)
	if err = os.Rename(tmpName, path); err != nil && !os.IsExist(err) {
		return err
	}
	public := newCheckpoint(cp.Table, opts)
	public.ProtectedState = &protectedStateRef{Path: name, SHA256: digest, Size: int64(*count)}
	return saveCheckpoint(checkpointPath, public)
}

func writeProtectedRecord(w io.Writer, policy *governance.EffectivePolicy, identity string, kind byte, ordinal uint64, plaintext []byte) error {
	sealed, err := policy.ProtectCheckpointRecord(identity, fmt.Sprintf("%d", kind), ordinal, plaintext)
	if err != nil {
		return err
	}
	var header [13]byte
	header[0] = kind
	binary.BigEndian.PutUint64(header[1:9], ordinal)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(sealed)))
	if _, err = w.Write(header[:]); err == nil {
		_, err = w.Write(sealed)
	}
	return err
}

func loadGovernedState(work string, cp captureCheckpoint, opts model.CaptureOptions) (governedResumeState, []governance.CohortSelectionState, error) {
	var state governedResumeState
	ref := cp.ProtectedState
	if ref == nil || filepath.Base(ref.Path) != ref.Path || ref.Size <= int64(len(governedStateMagic)) {
		return state, nil, fmt.Errorf("invalid state reference")
	}
	path := filepath.Join(work, ref.Path)
	f, err := os.Open(path)
	if err != nil {
		return state, nil, err
	}
	defer f.Close()
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(f, ref.Size+1), h)
	magic := make([]byte, len(governedStateMagic))
	if _, err = io.ReadFull(r, magic); err != nil || string(magic) != governedStateMagic {
		return state, nil, fmt.Errorf("invalid state header")
	}
	kind, ordinal, payload, err := readProtectedRecord(r, opts.Governance, checkpointProtectionIdentity(cp))
	if err != nil || kind != 1 || ordinal != 0 || json.Unmarshal(payload, &state) != nil {
		return state, nil, fmt.Errorf("invalid state metadata")
	}
	if state.SelectionCount < 0 {
		return state, nil, fmt.Errorf("invalid selection count")
	}
	selection := make([]governance.CohortSelectionState, 0, state.SelectionCount)
	for expected := 1; expected <= state.SelectionCount; expected++ {
		kind, ordinal, payload, err = readProtectedRecord(r, opts.Governance, checkpointProtectionIdentity(cp))
		if err != nil || kind != 2 || ordinal != uint64(expected) || len(payload) < 8 {
			return state, nil, fmt.Errorf("invalid candidate record")
		}
		rankLen := int(binary.BigEndian.Uint32(payload[:4]))
		if rankLen < 0 || 8+rankLen > len(payload) {
			return state, nil, fmt.Errorf("invalid candidate")
		}
		valueLen := int(binary.BigEndian.Uint32(payload[4+rankLen : 8+rankLen]))
		if valueLen < 0 || 8+rankLen+valueLen != len(payload) {
			return state, nil, fmt.Errorf("invalid candidate")
		}
		selection = append(selection, governance.CohortSelectionState{Rank: payload[4 : 4+rankLen], Value: payload[8+rankLen:]})
	}
	kind, ordinal, payload, err = readProtectedRecord(r, opts.Governance, checkpointProtectionIdentity(cp))
	if err != nil || kind != 255 || ordinal != uint64(state.SelectionCount+1) || len(payload) != 0 {
		return state, nil, fmt.Errorf("missing terminal record")
	}
	extra := make([]byte, 1)
	n, readErr := r.Read(extra)
	if n != 0 || readErr != io.EOF {
		return state, nil, fmt.Errorf("trailing state data")
	}
	if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return state, nil, fmt.Errorf("state digest mismatch")
	}
	if info, statErr := f.Stat(); statErr != nil || info.Size() != ref.Size {
		return state, nil, fmt.Errorf("state size mismatch")
	}
	if err = validateGovernedState(state, selection, cp, opts); err != nil {
		return state, nil, err
	}
	return state, selection, nil
}

func readProtectedRecord(r io.Reader, policy *governance.EffectivePolicy, identity string) (byte, uint64, []byte, error) {
	var header [13]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, 0, nil, err
	}
	kind, ordinal, size := header[0], binary.BigEndian.Uint64(header[1:9]), binary.BigEndian.Uint32(header[9:13])
	if size < 28 || size > 128<<20 {
		return 0, 0, nil, fmt.Errorf("invalid protected record size")
	}
	sealed := make([]byte, int(size))
	if _, err := io.ReadFull(r, sealed); err != nil {
		return 0, 0, nil, err
	}
	plain, err := policy.UnprotectCheckpointRecord(identity, fmt.Sprintf("%d", kind), ordinal, sealed)
	return kind, ordinal, plain, err
}

func applyGovernedState(cp *captureCheckpoint, state governedResumeState) {
	cp.LastKey, cp.ScanComplete, cp.Runs = state.LastKey, state.ScanComplete, state.Runs
	cp.Items, cp.Pages, cp.Truncated = state.Items, state.Pages, state.Truncated
	cp.SourceBytes, cp.ConsumedCapacity, cp.GovernanceCounts = state.SourceBytes, state.ConsumedCapacity, state.GovernanceCounts
	cp.ScannedItems = state.ScannedItems
}

func validateGovernedState(state governedResumeState, selection []governance.CohortSelectionState, cp captureCheckpoint, opts model.CaptureOptions) error {
	if state.Items < 0 || state.ScannedItems < state.Items || state.Pages < 0 || state.SourceBytes < 0 || state.ConsumedCapacity < 0 || math.IsNaN(state.ConsumedCapacity) || math.IsInf(state.ConsumedCapacity, 0) || state.SelectionCount != len(selection) {
		return fmt.Errorf("invalid counters")
	}
	if len(state.LastKey) != 0 {
		decoded, err := decodeKey(state.LastKey)
		if err != nil {
			return fmt.Errorf("invalid cursor")
		}
		canonical, err := encodeKey(decoded)
		if err != nil || !bytes.Equal(canonical, state.LastKey) {
			return fmt.Errorf("non-canonical cursor")
		}
	}
	if state.ScanComplete && len(state.LastKey) != 0 {
		return fmt.Errorf("complete state has cursor")
	}
	if state.Truncated && !state.ScanComplete {
		return fmt.Errorf("truncated state is incomplete")
	}
	test := cp
	applyGovernedState(&test, state)
	test.CohortSelection = selection
	if err := validateCohortCheckpoint(test, opts); err != nil {
		return err
	}
	knownRules := make(map[string]bool)
	for _, rule := range opts.Governance.Rules {
		if rule.Service == governance.ServiceDynamoDB && rule.Resource == cp.Table {
			knownRules[rule.ID] = true
		}
	}
	for id, count := range state.GovernanceCounts {
		if !knownRules[id] || count < 0 {
			return fmt.Errorf("invalid governance counter")
		}
	}
	var cohort *governance.Cohort
	for i := range opts.Governance.Cohorts {
		if opts.Governance.Cohorts[i].Resource == cp.Table {
			cohort = &opts.Governance.Cohorts[i]
			break
		}
	}
	if cohort != nil {
		if len(state.Runs) != 0 || len(selection) != min(state.Items, cohort.Limit) {
			return fmt.Errorf("invalid cohort state cardinality")
		}
		seen := make(map[string]bool, len(selection))
		for _, candidate := range selection {
			fingerprint := string(candidate.Rank) + "\x00" + string(candidate.Value)
			if seen[fingerprint] {
				return fmt.Errorf("duplicate cohort candidate")
			}
			seen[fingerprint] = true
		}
	} else if len(selection) != 0 {
		return fmt.Errorf("unexpected cohort state")
	}
	for _, run := range state.Runs {
		if info, e := os.Stat(run.Path); e != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("missing run")
		}
		got, e := sumRun(run.Path)
		if e != nil || got.SHA256 != run.SHA256 || got.Size != run.Size {
			return fmt.Errorf("invalid run")
		}
	}
	return nil
}

func checkpointProtectionIdentity(cp captureCheckpoint) string {
	return fmt.Sprintf("dynamodb\x00%s\x00%s\x00%d\x00%d\x00%s", cp.Table, cp.Mode, cp.MaxItems, cp.MaxPages, cp.GovernanceIdentity)
}

func validateCohortCheckpoint(cp captureCheckpoint, opts model.CaptureOptions) error {
	var cohort *governance.Cohort
	for i := range opts.Governance.Cohorts {
		if opts.Governance.Cohorts[i].Resource == cp.Table {
			cohort = &opts.Governance.Cohorts[i]
			break
		}
	}
	if cohort == nil {
		if len(cp.CohortSelection) != 0 {
			return fmt.Errorf("unexpected cohort state")
		}
		return nil
	}
	if len(cp.CohortSelection) > cohort.Limit {
		return fmt.Errorf("too many cohort candidates")
	}
	if cp.Items < len(cp.CohortSelection) || cp.Items < 0 || cp.Pages < 0 || cp.SourceBytes < 0 || cp.ConsumedCapacity < 0 {
		return fmt.Errorf("invalid cohort checkpoint counters")
	}
	var retainedBytes int64
	for _, candidate := range cp.CohortSelection {
		retainedBytes += int64(len(candidate.Rank) + len(candidate.Value))
		if len(candidate.Rank) != sha256.Size {
			return fmt.Errorf("invalid cohort rank")
		}
		item, err := decodeKey(candidate.Value)
		if err != nil {
			return err
		}
		canonical, err := CanonicalItem(item)
		if err != nil || !bytes.Equal(canonical, candidate.Value) {
			return fmt.Errorf("non-canonical cohort item")
		}
	}
	maxBytes := cohort.MaxRetainedBytes
	if maxBytes == 0 {
		maxBytes = governance.DefaultCohortMaxRetainedBytes
	}
	if retainedBytes > maxBytes {
		return fmt.Errorf("cohort retained bytes exceed limit")
	}
	return nil
}

func newCheckpoint(table string, opts model.CaptureOptions) captureCheckpoint {
	mode := opts.Mode
	if mode == "" {
		mode = "bounded"
	}
	return captureCheckpoint{Version: captureCheckpointVersion, Table: table, Mode: mode, MaxItems: opts.Limits.MaxItems, MaxPages: opts.Limits.MaxPages, GovernanceIdentity: governance.IdentityOf(opts.Governance)}
}

// captureIdentityMatches requires identical capture options on resume: the
// scanned pages and the last evaluated key are only meaningful for the same
// mode and limits, so a changed definition must start from a fresh checkpoint.
func captureIdentityMatches(cp captureCheckpoint, opts model.CaptureOptions) bool {
	mode := opts.Mode
	if mode == "" {
		mode = "bounded"
	}
	return cp.Mode == mode && cp.MaxItems == opts.Limits.MaxItems && cp.MaxPages == opts.Limits.MaxPages && cp.GovernanceIdentity == governance.IdentityOf(opts.Governance)
}
func sumRun(path string) (checkpointRun, error) {
	f, err := os.Open(path)
	if err != nil {
		return checkpointRun{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return checkpointRun{Path: path, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, err
}
func saveCheckpoint(path string, cp captureCheckpoint) error {
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return storage.WriteFileSync(path, b)
}

// scaleEstimate scales value by numer/denom without overflowing int64. Disk
// preflight only needs an order-of-magnitude estimate, so float64 arithmetic
// (with its 2^53 precision) is more than sufficient.
func scaleEstimate(value, numer, denom int64) int64 {
	if value <= 0 || numer <= 0 || denom <= 0 {
		return 0
	}
	return int64(float64(value) * float64(numer) / float64(denom))
}
func encodeKey(key map[string]types.AttributeValue) (json.RawMessage, error) {
	if len(key) == 0 {
		return nil, nil
	}
	return CanonicalItem(key)
}
func decodeKey(data []byte) (map[string]types.AttributeValue, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]types.AttributeValue, len(raw))
	for k, v := range raw {
		a, err := decodeAttribute(v)
		if err != nil {
			return nil, err
		}
		out[k] = a
	}
	return out, nil
}
func decodeAttribute(data []byte) (types.AttributeValue, error) {
	var v map[string]json.RawMessage
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	for k, b := range v {
		switch k {
		case "S":
			var x string
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberS{Value: x}, nil
		case "N":
			var x string
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberN{Value: x}, nil
		case "B":
			var x string
			json.Unmarshal(b, &x)
			z, e := base64.StdEncoding.DecodeString(x)
			return &types.AttributeValueMemberB{Value: z}, e
		case "BOOL":
			var x bool
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberBOOL{Value: x}, nil
		case "NULL":
			var x bool
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberNULL{Value: x}, nil
		case "SS":
			var x []string
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberSS{Value: x}, nil
		case "NS":
			var x []string
			json.Unmarshal(b, &x)
			return &types.AttributeValueMemberNS{Value: x}, nil
		case "BS":
			var x []string
			json.Unmarshal(b, &x)
			z := make([][]byte, len(x))
			for i, s := range x {
				var e error
				z[i], e = base64.StdEncoding.DecodeString(s)
				if e != nil {
					return nil, e
				}
			}
			return &types.AttributeValueMemberBS{Value: z}, nil
		case "L":
			var x []json.RawMessage
			json.Unmarshal(b, &x)
			z := make([]types.AttributeValue, len(x))
			for i, q := range x {
				var e error
				z[i], e = decodeAttribute(q)
				if e != nil {
					return nil, e
				}
			}
			return &types.AttributeValueMemberL{Value: z}, nil
		case "M":
			var x map[string]json.RawMessage
			json.Unmarshal(b, &x)
			z := make(map[string]types.AttributeValue, len(x))
			for n, q := range x {
				a, e := decodeAttribute(q)
				if e != nil {
					return nil, e
				}
				z[n] = a
			}
			return &types.AttributeValueMemberM{Value: z}, nil
		}
	}
	return nil, fmt.Errorf("unsupported key attribute")
}
