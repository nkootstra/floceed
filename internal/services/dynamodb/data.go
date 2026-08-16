package dynamodb

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/storage"
)

const dynamoChunkBytes int64 = 64 << 20
const mergeFanIn = 64

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

// captureCheckpointVersion 3 records the capture definition (mode, limits)
// and whether a bounded scan was truncated alongside progress. A checkpoint
// can therefore only be resumed by a run with identical capture options while
// preserving the final result classification.
const captureCheckpointVersion = 3

type captureCheckpoint struct {
	Version          int             `json:"version"`
	Table            string          `json:"table"`
	Mode             string          `json:"mode"`
	MaxItems         int             `json:"max_items,omitempty"`
	MaxPages         int             `json:"max_pages,omitempty"`
	LastKey          json.RawMessage `json:"last_key,omitempty"`
	Runs             []checkpointRun `json:"runs"`
	Items            int             `json:"items"`
	Pages            int             `json:"pages"`
	Truncated        bool            `json:"truncated,omitempty"`
	SourceBytes      int64           `json:"source_bytes"`
	ConsumedCapacity float64         `json:"consumed_capacity"`
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
	if !full && (opts.Limits.MaxItems <= 0 || opts.Limits.MaxPages <= 0) {
		return DataResult{}, fmt.Errorf("positive DynamoDB item and page limits are required")
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
	var key map[string]types.AttributeValue
	if len(cp.LastKey) != 0 {
		key, err = decodeKey(cp.LastKey)
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
			if precision == "exact" {
				total = int64(r.Items)
			}
			opts.Progress(model.ProgressEvent{SchemaVersion: 1, Event: "progress", Operation: "pull", Phase: phase, Service: "dynamodb", Resource: table, CompletedRecords: int64(r.Items), TotalRecords: total, TotalPrecision: precision, Resumed: resumed})
		}
	}
	emit("capture", "estimated")
	detector := bundle.NewCredentialDetector()
	for full || (r.Pages < opts.Limits.MaxPages && r.Items < opts.Limits.MaxItems) {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		input := &awsddb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: key, ConsistentRead: aws.Bool(false), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal}
		if !full {
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
			if !full && r.Items >= opts.Limits.MaxItems {
				r.Truncated = true
				break
			}
			b, e := CanonicalItem(item)
			if e != nil {
				return r, e
			}
			_, _ = detector.Write(b)
			if e = detector.Err(); e != nil {
				return r, fmt.Errorf("%w in DynamoDB table %q", e, table)
			}
			rows = append(rows, b)
			r.Items++
			cp.SourceBytes += int64(len(b) + 1)
		}
		sort.Slice(rows, func(i, j int) bool { return string(rows[i]) < string(rows[j]) })
		run := filepath.Join(work, fmt.Sprintf("page-%09d.run", r.Pages))
		if err := writeRun(run, rows); err != nil {
			return r, err
		}
		runRef, e := sumRun(run)
		if e != nil {
			return r, e
		}
		cp.Runs = append(cp.Runs, runRef)
		cp.Items, cp.Pages, cp.ConsumedCapacity = r.Items, r.Pages, r.ConsumedCapacity
		key = o.LastEvaluatedKey
		cp.LastKey, err = encodeKey(key)
		if err != nil {
			return r, err
		}
		if !full && len(key) > 0 && (r.Pages == opts.Limits.MaxPages || r.Items == opts.Limits.MaxItems) {
			r.Truncated = true
		}
		cp.Truncated = r.Truncated
		if err := saveCheckpoint(cpPath, cp); err != nil {
			return r, err
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
	runPaths := make([]string, len(cp.Runs))
	for i, run := range cp.Runs {
		runPaths[i] = run.Path
	}
	merged, err := mergeRuns(ctx, work, runPaths)
	if err != nil {
		return r, err
	}
	dataset, err := writeDynamoChunks(ctx, table, merged, cp.SourceBytes, opts.Gzip, sink)
	if err != nil {
		return r, err
	}
	dataset.Resumed = resumed
	r.Dataset = dataset
	if len(dataset.Chunks) > 0 {
		r.Artifact = dataset.Chunks[0].Data
	}
	return r, nil
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
func (h lineHeap) Less(i, j int) bool { return string(h[i].line) < string(h[j].line) }
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

func writeDynamoChunks(ctx context.Context, table, merged string, sourceBytes int64, gzipEnabled bool, sink ArtifactWriter) (model.Dataset, error) {
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
	d := model.Dataset{Format: format, Records: 0, SourceBytes: sourceBytes, Consistency: "best_effort"}
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
	return cp, true, nil
}

func newCheckpoint(table string, opts model.CaptureOptions) captureCheckpoint {
	mode := opts.Mode
	if mode == "" {
		mode = "bounded"
	}
	return captureCheckpoint{Version: captureCheckpointVersion, Table: table, Mode: mode, MaxItems: opts.Limits.MaxItems, MaxPages: opts.Limits.MaxPages}
}

// captureIdentityMatches requires identical capture options on resume: the
// scanned pages and the last evaluated key are only meaningful for the same
// mode and limits, so a changed definition must start from a fresh checkpoint.
func captureIdentityMatches(cp captureCheckpoint, opts model.CaptureOptions) bool {
	mode := opts.Mode
	if mode == "" {
		mode = "bounded"
	}
	return cp.Mode == mode && cp.MaxItems == opts.Limits.MaxItems && cp.MaxPages == opts.Limits.MaxPages
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
