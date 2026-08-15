package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/model"
)

func (a *Adapter) captureObjects(ctx context.Context, bucket string, b *Bucket, snap *model.Snapshot, opts model.CaptureOptions) error {
	if opts.Limits.MaxObjects <= 0 || opts.Limits.MaxObjectBytes <= 0 || opts.Limits.MaxTotalBytes <= 0 {
		return fmt.Errorf("S3 data limits must all be positive")
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
	var total int64
	captured := 0
truncatedListing:
	for _, prefix := range prefixes {
		paginator := awss3.NewListObjectsV2Paginator(a.client, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
		for paginator.HasMorePages() {
			o, e := paginator.NextPage(ctx)
			if e != nil {
				return awsconfig.Classify(e, "list objects in S3 bucket "+bucket, "")
			}
			for _, v := range o.Contents {
				if captured >= opts.Limits.MaxObjects {
					snap.Findings = append(snap.Findings, truncated(bucket, "max_objects"))
					break truncatedListing
				}
				key, etag, size := aws.ToString(v.Key), aws.ToString(v.ETag), aws.ToInt64(v.Size)
				if size > opts.Limits.MaxObjectBytes {
					snap.Findings = append(snap.Findings, truncated(key, "max_object_bytes"))
					captured++
					continue
				}
				if size < 0 || total+size > opts.Limits.MaxTotalBytes {
					snap.Findings = append(snap.Findings, truncated(bucket, "max_total_bytes"))
					break truncatedListing
				}
				remaining := opts.Limits.MaxTotalBytes - total
				objectOptions := opts
				if remaining < objectOptions.Limits.MaxObjectBytes {
					objectOptions.Limits.MaxObjectBytes = remaining
				}
				object, ref, err := a.captureObject(ctx, bucket, key, etag, objectOptions, snap)
				if err != nil {
					return err
				}
				b.Objects = append(b.Objects, object)
				snap.Data = append(snap.Data, ref)
				total += object.Size
				captured++
			}
		}
	}
	return nil
}

func (a *Adapter) captureObject(ctx context.Context, bucket, key, etag string, opts model.CaptureOptions, snap *model.Snapshot) (Object, model.ArtifactRef, error) {
	o, e := a.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), IfMatch: emptyNil(etag), ChecksumMode: types.ChecksumModeEnabled})
	if e != nil {
		return Object{}, model.ArtifactRef{}, awsconfig.Classify(e, "download S3 object "+key, "")
	}
	obj, ref, e := writeObject(ctx, bucket, key, etag, o, opts)
	if closeErr := o.Body.Close(); e == nil {
		e = closeErr
	}
	if e != nil {
		return Object{}, model.ArtifactRef{}, e
	}
	if tags, e := a.client.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)}); e != nil {
		if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
			return Object{}, model.ArtifactRef{}, e
		}
		snap.Findings = append(snap.Findings, optionalFinding(key, "S3_OBJECT_TAGS_UNAVAILABLE", "tags", e))
	} else {
		for _, t := range tags.TagSet {
			obj.Tags = append(obj.Tags, Tag{aws.ToString(t.Key), aws.ToString(t.Value)})
		}
		sortTags(obj.Tags)
	}
	return obj, ref, nil
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
