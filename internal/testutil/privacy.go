package testutil

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
)

const maxDecodedArtifactBytes = 64 << 20

func SentinelVariants(sentinel []byte) [][]byte {
	variants := [][]byte{
		append([]byte(nil), sentinel...),
		[]byte(base64.StdEncoding.EncodeToString(sentinel)),
		[]byte(base64.RawStdEncoding.EncodeToString(sentinel)),
		[]byte(hex.EncodeToString(sentinel)),
		[]byte(url.QueryEscape(string(sentinel))),
	}
	if encoded, err := json.Marshal(string(sentinel)); err == nil && len(encoded) >= 2 {
		variants = append(variants, encoded[1:len(encoded)-1])
	}
	return uniqueBytes(variants)
}

// DecodeArtifacts returns the supplied bytes and payloads found in supported
// durable containers. It is deliberately bounded for safe use with failures.
func DecodeArtifacts(artifact []byte) [][]byte {
	return decodeArtifacts(artifact, 0)
}

func decodeArtifacts(artifact []byte, depth int) [][]byte {
	result := [][]byte{append([]byte(nil), artifact...)}
	if depth >= 4 || len(artifact) > maxDecodedArtifactBytes {
		return result
	}
	if reader, err := gzip.NewReader(bytes.NewReader(artifact)); err == nil {
		if decoded, readErr := io.ReadAll(io.LimitReader(reader, maxDecodedArtifactBytes+1)); readErr == nil && len(decoded) <= maxDecodedArtifactBytes {
			result = append(result, decodeArtifacts(decoded, depth+1)...)
		}
		_ = reader.Close()
	}
	tarReader := tar.NewReader(bytes.NewReader(artifact))
	for {
		_, err := tarReader.Next()
		if err != nil {
			break
		}
		decoded, readErr := io.ReadAll(io.LimitReader(tarReader, maxDecodedArtifactBytes+1))
		if readErr != nil || len(decoded) > maxDecodedArtifactBytes {
			break
		}
		result = append(result, decodeArtifacts(decoded, depth+1)...)
	}
	return uniqueBytes(result)
}

// WriteBoundarySpy captures every byte offered to a durable writer, including
// bytes from a write that the wrapped writer later rejects.
type WriteBoundarySpy struct {
	Writer io.Writer
	writes bytes.Buffer
}

func (s *WriteBoundarySpy) Write(p []byte) (int, error) {
	_, _ = s.writes.Write(p)
	if s.Writer == nil {
		return len(p), nil
	}
	return s.Writer.Write(p)
}

func (s *WriteBoundarySpy) Bytes() []byte { return append([]byte(nil), s.writes.Bytes()...) }

func uniqueBytes(values [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(values))
	result := make([][]byte, 0, len(values))
	for _, value := range values {
		key := string(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}
