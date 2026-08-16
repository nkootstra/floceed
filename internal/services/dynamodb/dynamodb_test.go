package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/governance"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/testutil"
)

type resumeClient struct {
	failOnce   bool
	firstCalls int
}

type millionRowClient struct {
	pages, pageSize int
	failAt          int
	failed          bool
	calls           int
}

func (f *millionRowClient) ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return nil, nil
}
func (f *millionRowClient) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return nil, nil
}
func (f *millionRowClient) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return nil, nil
}
func (f *millionRowClient) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, nil
}
func (f *millionRowClient) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	page := 0
	if value, ok := in.ExclusiveStartKey["page"].(*types.AttributeValueMemberN); ok {
		_, _ = fmt.Sscanf(value.Value, "%d", &page)
	}
	if page == f.failAt && !f.failed {
		f.failed = true
		return nil, context.Canceled
	}
	f.calls++
	items := make([]map[string]types.AttributeValue, f.pageSize)
	for i := range items {
		items[i] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", page*f.pageSize+i)}}
	}
	out := &dynamodb.ScanOutput{Items: items}
	if page+1 < f.pages {
		out.LastEvaluatedKey = map[string]types.AttributeValue{"page": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", page+1)}}
	}
	return out, nil
}

func (f *resumeClient) ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return nil, nil
}
func (f *resumeClient) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return nil, nil
}
func (f *resumeClient) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return nil, nil
}
func (f *resumeClient) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, nil
}
func (f *resumeClient) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if len(in.ExclusiveStartKey) == 0 {
		f.firstCalls++
		return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "b"}}}, LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}}, nil
	}
	if f.failOnce {
		f.failOnce = false
		return nil, context.Canceled
	}
	return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}}}, nil
}

func TestFullCaptureResumesWithoutRescanningDurablePages(t *testing.T) {
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	adapter := New(client)
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := adapter.captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	result, err := adapter.captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if client.firstCalls != 1 {
		t.Fatalf("durable first page was scanned %d times", client.firstCalls)
	}
	if !result.Dataset.Resumed || result.Dataset.Records != 2 || len(result.Dataset.Chunks) != 1 {
		t.Fatalf("dataset = %#v", result.Dataset)
	}
	b, err := os.ReadFile(filepath.Join(root, "artifacts", filepath.FromSlash(result.Dataset.Chunks[0].Data.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "{\"pk\":{\"S\":\"a\"}}\n{\"pk\":{\"S\":\"b\"}}\n" {
		t.Fatalf("resumed data = %q", b)
	}
}

func TestGovernedResumeRestoresAuditCountersFromProtectedCheckpoint(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("safe", []governance.Rule{{ID: "rule-001", Service: governance.ServiceDynamoDB, Resource: "orders", Target: governance.Target{Kind: governance.TargetDynamoDBAttribute, Path: "pk"}, Action: governance.ActionReplace, Replacement: "safe"}}, nil, bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	opts.GovernanceAudit = governance.NewAudit()
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err != nil {
		t.Fatal(err)
	}
	got := opts.GovernanceAudit.Snapshot()
	if len(got) != 1 || got[0].RuleID != "rule-001" || got[0].Count != governance.BucketOneToNine {
		t.Fatalf("resumed audit = %#v", got)
	}
	if counts := opts.GovernanceAudit.RuleCounts(); counts["rule-001"] != 2 {
		t.Fatalf("restored exact internal count = %#v, want 2", counts)
	}
}

func TestGovernedCohortResumeRestoresEligibleRetainedAndTruncationAudit(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "cohort-key", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 1}}, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	checkpointBytes, err := os.ReadFile(filepath.Join(opts.CheckpointDirectory, "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint captureCheckpoint
	if err := json.Unmarshal(checkpointBytes, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.ProtectedState == nil || len(checkpoint.CohortSelection) != 0 || len(checkpoint.Runs) != 0 || checkpoint.Items != 0 {
		t.Fatalf("public checkpoint leaked governed state: %#v", checkpoint)
	}
	opts.GovernanceAudit = governance.NewAudit()
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err != nil {
		t.Fatal(err)
	}
	cohorts := opts.GovernanceAudit.Result().Cohorts
	if len(cohorts) != 1 || cohorts[0].Eligible != governance.BucketOneToNine || cohorts[0].Retained != governance.BucketOneToNine || !cohorts[0].Truncated {
		t.Fatalf("resumed cohort audit = %#v", cohorts)
	}
}

func TestGovernedCheckpointEncryptsResumeKeyAndRejectsTampering(t *testing.T) {
	secret := bytes.Repeat([]byte{9}, 32)
	policy, err := governance.NewEffectivePolicy("safe", []governance.Rule{{ID: "rule-001", Service: governance.ServiceDynamoDB, Resource: "orders", Target: governance.Target{Kind: governance.TargetDynamoDBAttribute, Path: "email"}, Action: governance.ActionOmit}}, nil, secret)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	b, err := os.ReadFile(filepath.Join(opts.CheckpointDirectory, "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cp captureCheckpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		t.Fatal(err)
	}
	if len(cp.LastKey) != 0 || len(cp.ProtectedLastKey) != 0 || cp.ProtectedState == nil {
		t.Fatalf("checkpoint exposed cursor or omitted state reference: %#v", cp)
	}
	for _, forbidden := range []string{"last_key", "scan_complete", "cohort_selection", "items", "scanned_items", "pages", "truncated", "source_bytes", "consumed_capacity", "governance_rule_counts"} {
		if bytes.Contains(b, []byte(`"`+forbidden+`"`)) {
			t.Fatalf("public checkpoint contains sensitive field %q: %s", forbidden, b)
		}
	}
	statePath := filepath.Join(opts.CheckpointDirectory, cp.ProtectedState.Path)
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	state[len(state)-1] ^= 1
	if err := os.WriteFile(statePath, state, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err == nil || !strings.Contains(err.Error(), "corrupt DynamoDB capture checkpoint") || strings.Contains(err.Error(), "cipher") {
		t.Fatalf("tamper error = %v", err)
	}
}

func TestGovernedCohortRejectsCorruptCheckpointSelection(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "key-1", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 1}}, bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	_, _ = New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory})
	path := filepath.Join(opts.CheckpointDirectory, "checkpoint.json")
	b, _ := os.ReadFile(path)
	var cp captureCheckpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(opts.CheckpointDirectory, cp.ProtectedState.Path)
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	// Removing the authenticated terminal record must be diagnosed as
	// corruption rather than silently treating scanning as complete.
	if err := os.WriteFile(statePath, state[:len(state)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err == nil || !strings.Contains(err.Error(), "corrupt DynamoDB capture checkpoint") {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestFullCaptureResumesFinalizationWithoutAnotherScan(t *testing.T) {
	root := t.TempDir()
	f := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}}}}}
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	finalizeErr := errors.New("finalization failed")
	if _, err := New(f).captureData(context.Background(), "orders", opts, failingSink{err: finalizeErr}); !errors.Is(err, finalizeErr) {
		t.Fatalf("first error = %v", err)
	}
	result, err := New(f).captureData(context.Background(), "orders", opts, &memorySink{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Dataset.Records != 1 || len(f.scans) != 0 {
		t.Fatalf("result = %#v remaining scans = %d", result, len(f.scans))
	}
}

func TestGovernedCohortResumesTerminalFinalizationWithoutScanOrDuplicates(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "key-1", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 1}}, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	client := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}, {"pk": &types.AttributeValueMemberS{Value: "b"}}}}}}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	finalizeErr := errors.New("finalization failed")
	if _, err = New(client).captureData(context.Background(), "orders", opts, failingSink{err: finalizeErr}); !errors.Is(err, finalizeErr) {
		t.Fatalf("first error = %v", err)
	}
	sink := &memorySink{}
	opts.GovernanceAudit = governance.NewAudit()
	result, err := New(client).captureData(context.Background(), "orders", opts, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.scans) != 0 || result.Dataset.Records != 1 || bytes.Count(sink.data, []byte("\n")) != 1 {
		t.Fatalf("rescanned or duplicated terminal cohort: result=%#v data=%q", result, sink.data)
	}
}

func TestLargeCohortResumeIsBoundedAndMatchesUninterruptedCapture(t *testing.T) {
	testCohortResumeScale(t, 10)
}

func TestMillionRowCohortResumeIsBoundedAndMatchesUninterruptedCapture(t *testing.T) {
	if os.Getenv("FLOCEED_LARGE_TEST") != "1" {
		t.Skip("set FLOCEED_LARGE_TEST=1 to run the million-row capture exercise")
	}
	testCohortResumeScale(t, 1000)
}

func testCohortResumeScale(t *testing.T, pages int) {
	t.Helper()
	secret := bytes.Repeat([]byte{5}, 32)
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "key-1", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 10, MaxRetainedBytes: 16 << 10}}, secret)
	if err != nil {
		t.Fatal(err)
	}
	capture := func(client *millionRowClient, checkpoint string) (DataResult, *memorySink, governance.AuditSnapshot, error) {
		audit := governance.NewAudit()
		sink := &memorySink{}
		result, captureErr := New(client).captureData(context.Background(), "orders", model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: audit, CheckpointDirectory: checkpoint}, sink)
		return result, sink, audit.Result(), captureErr
	}
	root := t.TempDir()
	failAt := pages / 2
	resuming := &millionRowClient{pages: pages, pageSize: 1000, failAt: failAt}
	_, _, _, err = capture(resuming, filepath.Join(root, "resume"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interruption error = %v", err)
	}
	info, err := os.Stat(filepath.Join(root, "resume", "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 32<<10 {
		t.Fatalf("checkpoint size = %d, want <= 32 KiB", info.Size())
	}
	resumed, resumedSink, resumedAudit, err := capture(resuming, filepath.Join(root, "resume"))
	if err != nil {
		t.Fatal(err)
	}
	uninterrupted, uninterruptedSink, uninterruptedAudit, err := capture(&millionRowClient{pages: pages, pageSize: 1000, failAt: -1}, filepath.Join(root, "continuous"))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Items != pages*1000 || resumed.Dataset.Records != 10 || uninterrupted.Items != resumed.Items {
		t.Fatalf("resumed = %#v uninterrupted = %#v", resumed, uninterrupted)
	}
	if !bytes.Equal(resumedSink.data, uninterruptedSink.data) || !reflect.DeepEqual(resumedAudit, uninterruptedAudit) {
		t.Fatal("resumed output or audit differs from uninterrupted capture")
	}
	if resuming.calls > pages+governedCheckpointItemInterval/1000 {
		t.Fatalf("scan calls = %d, checkpoint replay exceeded one interval", resuming.calls)
	}
	states, err := filepath.Glob(filepath.Join(root, "resume", "governed-state-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	maxSnapshots := pages*1000/governedCheckpointItemInterval + 2
	if len(states) > maxSnapshots {
		t.Fatalf("protected snapshots = %d, want <= %d", len(states), maxSnapshots)
	}
}

func TestGovernedCheckpointWriteAmplificationIsBoundedNearRetainedByteLimit(t *testing.T) {
	const retainedLimit = int64(16 << 10)
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "key-1", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 10, MaxRetainedBytes: retainedLimit}}, bytes.Repeat([]byte{5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	items := make([]map[string]types.AttributeValue, 10)
	for i := range items {
		items[i] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)}, "payload": &types.AttributeValueMemberS{Value: strings.Repeat("x", 1400)}}
	}
	root := t.TempDir()
	client := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: items, LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: "9"}}}, {Items: nil}}}
	_, err = New(client).captureData(context.Background(), "orders", model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit(), CheckpointDirectory: root}, &memorySink{})
	if err != nil {
		t.Fatal(err)
	}
	states, err := filepath.Glob(filepath.Join(root, "governed-state-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, path := range states {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		total += info.Size()
	}
	if len(states) != 2 || total > 2*(retainedLimit+4<<10) {
		t.Fatalf("snapshots=%d cumulative bytes=%d, want 2 and <=%d", len(states), total, 2*(retainedLimit+4<<10))
	}
	t.Logf("protected checkpoint evidence: snapshots=%d cumulative_bytes=%d retained_limit=%d", len(states), total, retainedLimit)
}

func TestFullCaptureRejectsCorruptCheckpointRun(t *testing.T) {
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	adapter := New(client)
	opts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	_, _ = adapter.captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory})
	run := filepath.Join(opts.CheckpointDirectory, "page-000000001.run")
	if err := os.WriteFile(run, []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err == nil || !strings.Contains(err.Error(), "corrupt DynamoDB capture checkpoint") {
		t.Fatalf("error = %v", err)
	}
}

func TestFullCaptureRejectsChangedMode(t *testing.T) {
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	adapter := New(client)
	fullOpts := model.CaptureOptions{Mode: "full", ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := adapter.captureData(context.Background(), "orders", fullOpts, directoryWriter{root: fullOpts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	boundedOpts := model.CaptureOptions{Mode: "bounded", Limits: model.DataLimits{MaxItems: 1, MaxPages: 1}, ArtifactDirectory: fullOpts.ArtifactDirectory, CheckpointDirectory: fullOpts.CheckpointDirectory}
	if _, err := adapter.captureData(context.Background(), "orders", boundedOpts, directoryWriter{root: boundedOpts.ArtifactDirectory}); err == nil || !strings.Contains(err.Error(), "incompatible DynamoDB capture checkpoint") {
		t.Fatalf("changed-options error = %v", err)
	}
}

func TestPlanOwnsDynamoDBSelectionOptionsAndIAM(t *testing.T) {
	gzipEnabled := false
	project := config.Project{Resources: config.Resources{DynamoDB: []config.DynamoDBResource{{Name: "orders", PreserveProvisioned: true, Data: &config.DynamoDBDataPolicy{Enabled: true, MaxItems: 7, MaxPages: 8, Gzip: &gzipEnabled}}}}}
	contribution := New(nil).Plan(project, true)
	if len(contribution.Selections) != 1 {
		t.Fatalf("selections = %#v", contribution.Selections)
	}
	selection := contribution.Selections[0]
	if selection.Resource != (model.ResourceRef{Service: "dynamodb", Type: "table", ID: "orders"}) {
		t.Fatalf("resource = %#v", selection.Resource)
	}
	if !selection.Options.IncludeData || !selection.Options.PreserveProvisioned || selection.Options.Gzip || selection.Options.Limits != (model.DataLimits{MaxItems: 7, MaxPages: 8}) {
		t.Fatalf("options = %#v", selection.Options)
	}
	wantActions := map[string]bool{"dynamodb:ListTables": true, "dynamodb:DescribeTable": true, "dynamodb:DescribeTimeToLive": true, "dynamodb:ListTagsOfResource": true, "dynamodb:Scan": true}
	for _, action := range contribution.RequiredIAMActions {
		delete(wantActions, action)
	}
	if len(wantActions) != 0 {
		t.Fatalf("required IAM actions missing: %v", wantActions)
	}
}

type fakeClient struct {
	list      []*dynamodb.ListTablesOutput
	described map[string]*dynamodb.DescribeTableOutput
	scans     []*dynamodb.ScanOutput
}

type memorySink struct {
	path string
	data []byte
}

type failingSink struct{ err error }

func (s failingSink) WriteArtifact(context.Context, string, func(io.Writer) error) (model.ArtifactRef, error) {
	return model.ArtifactRef{}, s.err
}

func (s *memorySink) WriteArtifact(_ context.Context, path string, write func(io.Writer) error) (model.ArtifactRef, error) {
	var b bytes.Buffer
	if err := write(&b); err != nil {
		return model.ArtifactRef{}, err
	}
	s.path = path
	s.data = append([]byte(nil), b.Bytes()...)
	return model.ArtifactRef{Path: path, Size: int64(b.Len())}, nil
}

func (f *fakeClient) ListTables(_ context.Context, in *dynamodb.ListTablesInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	n := 0
	if in.ExclusiveStartTableName != nil {
		n = 1
	}
	return f.list[n], nil
}
func (f *fakeClient) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return f.described[*in.TableName], nil
}
func (f *fakeClient) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return &dynamodb.DescribeTimeToLiveOutput{}, nil
}
func (f *fakeClient) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return &dynamodb.ListTagsOfResourceOutput{}, nil
}
func (f *fakeClient) Scan(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	o := f.scans[0]
	f.scans = f.scans[1:]
	return o, nil
}

func TestDiscoverPaginatesAndSorts(t *testing.T) {
	f := &fakeClient{list: []*dynamodb.ListTablesOutput{{TableNames: []string{"z"}, LastEvaluatedTableName: aws.String("z")}, {TableNames: []string{"a"}}}, described: map[string]*dynamodb.DescribeTableOutput{}}
	for _, n := range []string{"a", "z"} {
		f.described[n] = &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableName: aws.String(n), TableArn: aws.String("arn:" + n), ItemCount: aws.Int64(2), TableSizeBytes: aws.Int64(3)}}
	}
	r, err := New(f).Discover(context.Background(), model.SourceScope{Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{r.Resources[0].Name, r.Resources[1].Name}; !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatal(got)
	}
}

func TestCanonicalItemSortsMapsAndSets(t *testing.T) {
	b, err := CanonicalItem(map[string]types.AttributeValue{"set": &types.AttributeValueMemberSS{Value: []string{"z", "a"}}, "m": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"b": &types.AttributeValueMemberN{Value: "2"}, "a": &types.AttributeValueMemberBOOL{Value: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if string(b) != "{\"m\":{\"M\":{\"a\":{\"BOOL\":true},\"b\":{\"N\":\"2\"}}},\"set\":{\"SS\":[\"a\",\"z\"]}}" {
		t.Fatalf("%s", b)
	}
}

func TestNormalizeDefaultsToPayPerRequest(t *testing.T) {
	in := &types.TableDescription{TableName: aws.String("orders"), BillingModeSummary: &types.BillingModeSummary{BillingMode: types.BillingModeProvisioned}, AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}}, KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}}
	got := Normalize(in, nil, nil, false)
	if got.BillingMode != "PAY_PER_REQUEST" || got.SourceBillingMode != "PROVISIONED" {
		t.Fatalf("%+v", got)
	}
}

func TestCaptureDataHonorsItemLimitAndIsDeterministic(t *testing.T) {
	items := []map[string]types.AttributeValue{
		{"pk": &types.AttributeValueMemberS{Value: "z"}},
		{"pk": &types.AttributeValueMemberS{Value: "a"}},
		{"pk": &types.AttributeValueMemberS{Value: "ignored"}},
	}
	f := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: items, LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "z"}}}}}
	sink := &memorySink{}
	result, err := New(f).CaptureData(context.Background(), "orders", model.DataLimits{MaxItems: 2, MaxPages: 1}, false, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Items != 2 || !result.Truncated {
		t.Fatalf("%+v", result)
	}
	want := "{\"pk\":{\"S\":\"a\"}}\n{\"pk\":{\"S\":\"z\"}}\n"
	if string(sink.data) != want {
		t.Fatalf("got %q want %q", sink.data, want)
	}
}

func TestGovernedCaptureTransformsNestedAttributeBeforeDurableWritesAndCredentialScan(t *testing.T) {
	const protected = "AKIAIOSFODNN7EXAMPLE"
	policy, err := governance.NewEffectivePolicy("safe", []governance.Rule{{
		ID: "rule-1", Service: governance.ServiceDynamoDB, Resource: "orders",
		Target: governance.Target{Kind: governance.TargetDynamoDBAttribute, Path: "customer.credentials"},
		Action: governance.ActionReplace, Replacement: "redacted",
	}}, nil, bytes.Repeat([]byte{6}, 32))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	f := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: []map[string]types.AttributeValue{{
		"pk": &types.AttributeValueMemberS{Value: "1"},
		"customer": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"credentials": &types.AttributeValueMemberS{Value: protected},
		}},
	}}}}}
	opts := model.CaptureOptions{Mode: "full", Governance: policy, ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	result, err := New(f).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(opts.ArtifactDirectory, filepath.FromSlash(result.Dataset.Chunks[0].Data.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(protected)) || !bytes.Contains(data, []byte("redacted")) {
		t.Fatalf("dataset leaked protected value or omitted replacement: %s", data)
	}
	err = filepath.Walk(opts.CheckpointDirectory, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() {
			return walkErr
		}
		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, decoded := range testutil.DecodeArtifacts(payload) {
			for _, variant := range testutil.SentinelVariants([]byte(protected)) {
				if bytes.Contains(decoded, variant) {
					t.Errorf("protected value persisted in %s", filepath.Base(path))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGovernedCohortProducesStableDatasetAcrossScanOrders(t *testing.T) {
	secret := bytes.Repeat([]byte{0x31}, 32)
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{
		Resource: "orders", KeyID: "cohort-1", Scope: "project", Algorithm: governance.CohortRankAlgorithm,
		KeyPaths: []string{"pk"}, Limit: 2,
	}}, secret)
	if err != nil {
		t.Fatal(err)
	}
	items := []map[string]types.AttributeValue{
		{"pk": &types.AttributeValueMemberS{Value: "a"}},
		{"pk": &types.AttributeValueMemberS{Value: "b"}},
		{"pk": &types.AttributeValueMemberS{Value: "c"}},
	}
	capture := func(items []map[string]types.AttributeValue) []byte {
		t.Helper()
		f := &fakeClient{scans: []*dynamodb.ScanOutput{{Items: items}}}
		sink := &memorySink{}
		_, err := New(f).captureData(context.Background(), "orders", model.CaptureOptions{Mode: "full", Governance: policy}, sink)
		if err != nil {
			t.Fatal(err)
		}
		return append([]byte(nil), sink.data...)
	}
	forward := capture(items)
	reverse := capture([]map[string]types.AttributeValue{items[2], items[1], items[0]})
	if !bytes.Equal(forward, reverse) {
		t.Fatalf("forward = %s, reverse = %s", forward, reverse)
	}
	if lines := bytes.Count(forward, []byte("\n")); lines != 2 {
		t.Fatalf("selected records = %d, want 2", lines)
	}
}

func TestGovernedCohortDatasetTotalsDescribeRetainedOutput(t *testing.T) {
	policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{Resource: "orders", KeyID: "cohort-1", Algorithm: governance.CohortRankAlgorithm, KeyPaths: []string{"pk"}, Limit: 1}}, bytes.Repeat([]byte{0x41}, 32))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	result, err := New(&fakeClient{scans: []*dynamodb.ScanOutput{{Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}, {"pk": &types.AttributeValueMemberS{Value: "b"}}, {"pk": &types.AttributeValueMemberS{Value: "c"}}}}}}).captureData(context.Background(), "orders", model.CaptureOptions{Mode: "full", Governance: policy, GovernanceAudit: governance.NewAudit()}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dataset.SourceBytes != int64(len(sink.data)) || result.Dataset.SourceBytes != result.Dataset.Chunks[0].SourceBytes {
		t.Fatalf("dataset totals=%#v output bytes=%d", result.Dataset, len(sink.data))
	}
	manifest := model.Manifest{SchemaVersion: 3, Snapshots: []model.Snapshot{{Resource: model.ResourceRef{Service: "dynamodb", ID: "orders"}, Service: "dynamodb", StructureVersion: 1, Structure: json.RawMessage(`{"name":"orders","attribute_definitions":[],"key_schema":[{}],"billing_mode":"PAY_PER_REQUEST"}`), Dataset: &result.Dataset}}}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("retained cohort manifest is invalid: %v", err)
	}
}

func TestGovernedCaptureRejectsResumeAfterSecretRotation(t *testing.T) {
	makePolicy := func(fill byte) *governance.EffectivePolicy {
		t.Helper()
		policy, err := governance.NewEffectivePolicy("safe", nil, []governance.Cohort{{
			Resource: "orders", KeyID: "cohort-1", Algorithm: governance.CohortRankAlgorithm,
			KeyPaths: []string{"pk"}, Limit: 1,
		}}, bytes.Repeat([]byte{fill}, 32))
		if err != nil {
			t.Fatal(err)
		}
		return policy
	}
	root := t.TempDir()
	client := &resumeClient{failOnce: true}
	opts := model.CaptureOptions{Mode: "full", Governance: makePolicy(1), ArtifactDirectory: filepath.Join(root, "artifacts"), CheckpointDirectory: filepath.Join(root, "checkpoint")}
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first capture error = %v", err)
	}
	opts.Governance = makePolicy(2)
	if _, err := New(client).captureData(context.Background(), "orders", opts, directoryWriter{root: opts.ArtifactDirectory}); err == nil || !strings.Contains(err.Error(), "incompatible DynamoDB capture checkpoint") {
		t.Fatalf("rotation error = %v", err)
	}
}

func TestBoundedCaptureResumePreservesTruncationAfterFinalizationFailure(t *testing.T) {
	root := t.TempDir()
	f := &fakeClient{scans: []*dynamodb.ScanOutput{{
		Items:            []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}},
		LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	}}}
	adapter := New(f)
	opts := model.CaptureOptions{
		Mode:                "bounded",
		Limits:              model.DataLimits{MaxItems: 1, MaxPages: 1},
		ArtifactDirectory:   filepath.Join(root, "artifacts"),
		CheckpointDirectory: filepath.Join(root, "checkpoint"),
	}
	finalizeErr := errors.New("finalization failed")
	if _, err := adapter.captureData(context.Background(), "orders", opts, failingSink{err: finalizeErr}); !errors.Is(err, finalizeErr) {
		t.Fatalf("first capture error = %v, want %v", err, finalizeErr)
	}

	result, err := adapter.captureData(context.Background(), "orders", opts, &memorySink{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated {
		t.Fatalf("resumed result = %#v, want truncated", result)
	}
	if len(f.scans) != 0 {
		t.Fatalf("resume performed an unexpected scan")
	}
}
