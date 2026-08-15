package dynamodb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/nkootstra/floceed/internal/model"
)

type ArtifactWriter interface {
	WriteArtifact(context.Context, string, func(io.Writer) error) (model.ArtifactRef, error)
}

type directoryWriter struct{ root string }

type byteCounter int64

func (c *byteCounter) Write(p []byte) (int, error) {
	*c += byteCounter(len(p))
	return len(p), nil
}

func (w directoryWriter) WriteArtifact(ctx context.Context, name string, write func(io.Writer) error) (model.ArtifactRef, error) {
	clean := filepath.Clean(filepath.FromSlash(name))
	if w.root == "" || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return model.ArtifactRef{}, fmt.Errorf("unsafe artifact path %q", name)
	}
	select {
	case <-ctx.Done():
		return model.ArtifactRef{}, ctx.Err()
	default:
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
	Items, Pages     int
	ConsumedCapacity float64
	Truncated        bool
}

func (a *Adapter) CaptureData(ctx context.Context, table string, limits model.DataLimits, gzipEnabled bool, sink ArtifactWriter) (DataResult, error) {
	if limits.MaxItems <= 0 || limits.MaxPages <= 0 {
		return DataResult{}, fmt.Errorf("positive DynamoDB item and page limits are required")
	}
	var rows [][]byte
	var key map[string]types.AttributeValue
	r := DataResult{}
	for r.Pages < limits.MaxPages && r.Items < limits.MaxItems {
		o, err := a.client.Scan(ctx, &awsddb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: key, ConsistentRead: aws.Bool(false), Limit: aws.Int32(int32(limits.MaxItems - r.Items)), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
		if err != nil {
			return r, err
		}
		r.Pages++
		if o.ConsumedCapacity != nil {
			r.ConsumedCapacity += aws.ToFloat64(o.ConsumedCapacity.CapacityUnits)
		}
		for _, item := range o.Items {
			if r.Items >= limits.MaxItems {
				r.Truncated = true
				break
			}
			b, err := CanonicalItem(item)
			if err != nil {
				return r, err
			}
			rows = append(rows, b)
			r.Items++
		}
		key = o.LastEvaluatedKey
		if len(key) == 0 {
			break
		}
		if r.Pages == limits.MaxPages || r.Items == limits.MaxItems {
			r.Truncated = true
			break
		}
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i], rows[j]) < 0 })
	sum := sha256.Sum256([]byte(table))
	ext := ".ndjson"
	media := "application/x-ndjson"
	if gzipEnabled {
		ext += ".gz"
		media = "application/gzip"
	}
	rel := "bundle/data/dynamodb/" + hex.EncodeToString(sum[:16]) + ext
	art, err := sink.WriteArtifact(ctx, rel, func(w io.Writer) error {
		var dst io.Writer = w
		var gz *gzip.Writer
		if gzipEnabled {
			gz = gzip.NewWriter(w)
			gz.Header.ModTime = time.Unix(0, 0)
			gz.Header.Name = ""
			gz.Header.Comment = ""
			dst = gz
		}
		for _, row := range rows {
			if _, e := dst.Write(append(row, '\n')); e != nil {
				return e
			}
		}
		if gz != nil {
			return gz.Close()
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	art.MediaType = media
	r.Artifact = art
	return r, nil
}
