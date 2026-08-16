package testutil

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"testing"
)

func TestSentinelVariantsIncludeCommonDurableEncodings(t *testing.T) {
	sentinel := []byte("protected@example.test")
	variants := SentinelVariants(sentinel)
	wants := [][]byte{sentinel, []byte(base64.StdEncoding.EncodeToString(sentinel)), []byte("protected@example.test")}
	for _, want := range wants {
		if !containsBytes(variants, want) {
			t.Fatalf("variants do not include %q: %#v", want, variants)
		}
	}
}

func TestDecodeArtifactsRecursesIntoGzipAndTar(t *testing.T) {
	var packed bytes.Buffer
	zipper := gzip.NewWriter(&packed)
	tarWriter := tar.NewWriter(zipper)
	payload := []byte("protected@example.test")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "rows.ndjson", Mode: 0o600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zipper.Close(); err != nil {
		t.Fatal(err)
	}

	decoded := DecodeArtifacts(packed.Bytes())
	if !containsBytes(decoded, payload) {
		t.Fatalf("decoded artifacts do not include payload: %#v", decoded)
	}
}

func containsBytes(values [][]byte, want []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, want) {
			return true
		}
	}
	return false
}
