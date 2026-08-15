package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

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
