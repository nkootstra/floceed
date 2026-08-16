package governance

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

type boundedZeroReader struct {
	remaining int64
	maxRead   int
}

func (r *boundedZeroReader) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		return 0, errors.New("unbounded read")
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	clear(p)
	r.remaining -= int64(len(p))
	return len(p), nil
}

type failOnRead struct{}

func (failOnRead) Read([]byte) (int, error) { return 0, errors.New("source was read") }

type protectedErrorReader struct{}

func (protectedErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("failed near person-secret@example.test")
}

func TestEngineAppliesPublicHash(t *testing.T) {
	engine := NewEngine("safe", nil)
	rule := Rule{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "email"}, Action: ActionHash}

	result, err := engine.Apply(rule, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Omit {
		t.Fatal("hash unexpectedly omitted value")
	}
	want := "hash/v1:" + hex.EncodeToString([]byte{0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9, 0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62, 0x93, 0x8b, 0x98, 0x24})
	if got := string(result.Value); got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestEngineStreamsLargeHashWithBoundedReads(t *testing.T) {
	source := &boundedZeroReader{remaining: 32 << 20, maxRead: 32 << 10}
	var output bytes.Buffer
	rule := Rule{ID: "rule-001", Service: ServiceS3, Resource: "assets", Target: Target{Kind: TargetS3TextBody}, Action: ActionHash}
	omit, err := NewEngine("safe", nil).ApplyReader(rule, source, &output)
	if err != nil {
		t.Fatal(err)
	}
	if omit || source.remaining != 0 || !strings.HasPrefix(output.String(), HashAlgorithm+":") {
		t.Fatalf("omit=%v remaining=%d output=%q", omit, source.remaining, output.String())
	}
}

func TestEngineReplaceAndOmitDoNotReadSource(t *testing.T) {
	engine := NewEngine("safe", nil)
	for _, test := range []struct {
		action      Action
		replacement string
		want        string
		omit        bool
	}{{ActionReplace, "redacted", "redacted", false}, {ActionOmit, "", "", true}} {
		var output bytes.Buffer
		rule := Rule{ID: "rule-001", Service: ServiceS3, Resource: "assets", Target: Target{Kind: TargetS3TextBody}, Action: test.action, Replacement: test.replacement}
		omit, err := engine.ApplyReader(rule, failOnRead{}, &output)
		if err != nil {
			t.Fatal(err)
		}
		if omit != test.omit || output.String() != test.want {
			t.Fatalf("%s: omit=%v output=%q", test.action, omit, output.String())
		}
	}
}

func TestEngineStreamingErrorsDoNotExposeSourceErrors(t *testing.T) {
	rule := Rule{ID: "rule-001", Service: ServiceS3, Resource: "assets", Target: Target{Kind: TargetS3TextBody}, Action: ActionHash}
	_, err := NewEngine("safe", nil).ApplyReader(rule, protectedErrorReader{}, io.Discard)
	if !errors.Is(err, ErrInvalidTransformation) || strings.Contains(err.Error(), "person-secret@example.test") {
		t.Fatalf("error = %q", err)
	}
}

func TestEnginePseudonymErrorsDoNotExposeInputs(t *testing.T) {
	source := "person-secret@example.test"
	rule := Rule{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "customer.secret"}, Action: ActionPseudonymize, KeyID: "key-1"}

	_, err := NewEngine("safe", []byte("too-short-secret")).Apply(rule, []byte(source))
	if err == nil {
		t.Fatal("expected invalid transformation error")
	}
	for _, protected := range []string{source, rule.Target.Path, "too-short-secret"} {
		if strings.Contains(err.Error(), protected) {
			t.Fatalf("error %q exposes protected input", err)
		}
	}
}

func TestEnginePseudonymizesDeterministicallyWithinItsDomain(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	rule := Rule{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "email"}, Action: ActionPseudonymize, KeyID: "key-1", Scope: "fixture"}
	engine := NewEngine("safe", secret)

	first, err := engine.Apply(rule, []byte("person@example.test"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Apply(rule, []byte("person@example.test"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Value, second.Value) {
		t.Fatalf("same domain produced %q and %q", first.Value, second.Value)
	}
	if !bytes.HasPrefix(first.Value, []byte(PseudonymAlgorithm+":")) {
		t.Fatalf("pseudonym = %q, want version prefix", first.Value)
	}

	rotated := NewEngine("safe", []byte("abcdefghijklmnopqrstuvwxyzABCDEF"))
	third, err := rotated.Apply(rule, []byte("person@example.test"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Value, third.Value) {
		t.Fatal("secret rotation did not unlink pseudonym")
	}
}

func TestEngineOmitsAndReplacesValues(t *testing.T) {
	engine := NewEngine("safe", nil)
	base := Rule{ID: "rule-001", Service: ServiceDynamoDB, Resource: "orders", Target: Target{Kind: TargetDynamoDBAttribute, Path: "email"}}

	omit := base
	omit.Action = ActionOmit
	result, err := engine.Apply(omit, []byte("protected"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Omit || result.Value != nil {
		t.Fatalf("omit result = %#v", result)
	}

	replace := base
	replace.Action = ActionReplace
	replace.Replacement = "redacted"
	result, err = engine.Apply(replace, []byte("protected"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Omit || string(result.Value) != "redacted" {
		t.Fatalf("replace result = %#v", result)
	}
}
