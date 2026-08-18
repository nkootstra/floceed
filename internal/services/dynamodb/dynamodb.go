// Package dynamodb implements the source-side DynamoDB adapter. AWS SDK types
// are normalized here and never leave this package.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	ListTables(context.Context, *awsddb.ListTablesInput, ...func(*awsddb.Options)) (*awsddb.ListTablesOutput, error)
	DescribeTable(context.Context, *awsddb.DescribeTableInput, ...func(*awsddb.Options)) (*awsddb.DescribeTableOutput, error)
	DescribeTimeToLive(context.Context, *awsddb.DescribeTimeToLiveInput, ...func(*awsddb.Options)) (*awsddb.DescribeTimeToLiveOutput, error)
	ListTagsOfResource(context.Context, *awsddb.ListTagsOfResourceInput, ...func(*awsddb.Options)) (*awsddb.ListTagsOfResourceOutput, error)
	Scan(context.Context, *awsddb.ScanInput, ...func(*awsddb.Options)) (*awsddb.ScanOutput, error)
}
type Adapter struct{ client Client }

var _ catalog.Adapter = (*Adapter)(nil)

func New(client Client) *Adapter { return &Adapter{client: client} }
func (a *Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "dynamodb", DisplayName: "DynamoDB", Support: model.SupportPartial}
}

func (*Adapter) Plan(project config.Project, includeData bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{}
	if len(project.Resources.DynamoDB) == 0 {
		return contribution
	}
	contribution.RequiredIAMActions = []string{"dynamodb:ListTables", "dynamodb:DescribeTable", "dynamodb:DescribeTimeToLive", "dynamodb:ListTagsOfResource"}
	for _, resource := range project.Resources.DynamoDB {
		options := model.CaptureOptions{PreserveProvisioned: resource.PreserveProvisioned}
		if resource.Data != nil {
			options.IncludeData = resource.Data.Enabled
			options.Mode = string(resource.Data.Mode)
			if options.Mode == "" {
				options.Mode = string(config.DataModeBounded)
			}
			options.Limits = model.DataLimits{MaxItems: resource.Data.MaxItems, MaxPages: resource.Data.MaxPages}
			options.Gzip = resource.Data.Gzip == nil || *resource.Data.Gzip
			if includeData && resource.Data.Enabled {
				contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "dynamodb:Scan")
			}
		}
		contribution.Selections = append(contribution.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "dynamodb", Type: "table", ID: resource.Name}, Options: options})
	}
	return contribution
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

type AttributeDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type KeyElement struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type Projection struct {
	Type             string   `json:"type"`
	NonKeyAttributes []string `json:"non_key_attributes,omitempty"`
}
type SecondaryIndex struct {
	Name          string       `json:"name"`
	Keys          []KeyElement `json:"keys"`
	Projection    Projection   `json:"projection"`
	ReadCapacity  int64        `json:"read_capacity,omitempty"`
	WriteCapacity int64        `json:"write_capacity,omitempty"`
}
type Stream struct {
	Enabled  bool   `json:"enabled"`
	ViewType string `json:"view_type,omitempty"`
}
type TTL struct {
	Enabled   bool   `json:"enabled"`
	Attribute string `json:"attribute,omitempty"`
}
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type Table struct {
	Name              string                `json:"name"`
	Attributes        []AttributeDefinition `json:"attribute_definitions"`
	Keys              []KeyElement          `json:"key_schema"`
	GlobalIndexes     []SecondaryIndex      `json:"global_secondary_indexes,omitempty"`
	LocalIndexes      []SecondaryIndex      `json:"local_secondary_indexes,omitempty"`
	Stream            Stream                `json:"stream"`
	TTL               TTL                   `json:"ttl"`
	Tags              []Tag                 `json:"tags,omitempty"`
	BillingMode       string                `json:"billing_mode"`
	SourceBillingMode string                `json:"source_billing_mode"`
	ReadCapacity      int64                 `json:"read_capacity,omitempty"`
	WriteCapacity     int64                 `json:"write_capacity,omitempty"`
}

func (a *Adapter) Discover(ctx context.Context, scope model.SourceScope) (model.DiscoveryResult, error) {
	var names []string
	paginator := awsddb.NewListTablesPaginator(a.client, &awsddb.ListTablesInput{})
	for paginator.HasMorePages() {
		o, err := paginator.NextPage(ctx)
		if err != nil {
			return model.DiscoveryResult{}, awsconfig.Classify(err, "list DynamoDB tables", scope.Profile)
		}
		names = append(names, o.TableNames...)
	}
	sort.Strings(names)
	result := model.DiscoveryResult{}
	for _, name := range names {
		s := model.ResourceSummary{Ref: model.ResourceRef{Service: "dynamodb", Type: "table", ID: name}, Name: name, Region: scope.Region, Attributes: map[string]any{}}
		d, err := a.client.DescribeTable(ctx, &awsddb.DescribeTableInput{TableName: aws.String(name)})
		if err != nil {
			s.Findings = append(s.Findings, finding(name, "DYNAMODB_DESCRIBE_FAILED", err))
			result.Resources = append(result.Resources, s)
			continue
		}
		if d.Table != nil {
			s.Ref.ARN = aws.ToString(d.Table.TableArn)
			s.Attributes["item_count"] = aws.ToInt64(d.Table.ItemCount)
			s.Attributes["table_size_bytes"] = aws.ToInt64(d.Table.TableSizeBytes)
			if d.Table.StreamSpecification != nil {
				s.Attributes["stream_enabled"] = aws.ToBool(d.Table.StreamSpecification.StreamEnabled)
				s.Attributes["stream_view_type"] = string(d.Table.StreamSpecification.StreamViewType)
			}
		}
		if ttl, e := a.client.DescribeTimeToLive(ctx, &awsddb.DescribeTimeToLiveInput{TableName: aws.String(name)}); e != nil {
			s.Findings = append(s.Findings, finding(name, "DYNAMODB_TTL_UNAVAILABLE", e))
		} else if ttl.TimeToLiveDescription != nil {
			s.Attributes["ttl_status"] = string(ttl.TimeToLiveDescription.TimeToLiveStatus)
			s.Attributes["ttl_attribute"] = aws.ToString(ttl.TimeToLiveDescription.AttributeName)
		}
		if s.Ref.ARN != "" {
			if tags, e := a.client.ListTagsOfResource(ctx, &awsddb.ListTagsOfResourceInput{ResourceArn: aws.String(s.Ref.ARN)}); e != nil {
				s.Findings = append(s.Findings, finding(name, "DYNAMODB_TAGS_UNAVAILABLE", e))
			} else {
				s.Attributes["tag_count"] = len(tags.Tags)
			}
		}
		result.Resources = append(result.Resources, s)
	}
	return result, nil
}
func finding(resource, code string, err error) model.Finding {
	return model.Finding{Code: code, Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: resource, Message: err.Error(), Remediation: "Grant the corresponding read-only DynamoDB permission and retry."}
}

func (a *Adapter) Capture(ctx context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	d, err := a.client.DescribeTable(ctx, &awsddb.DescribeTableInput{TableName: aws.String(ref.ID)})
	if err != nil {
		return nil, awsconfig.Classify(err, "describe DynamoDB table "+ref.ID, scope.Profile)
	}
	if d.Table == nil {
		return nil, fmt.Errorf("DynamoDB table %q disappeared during capture", ref.ID)
	}
	var findings []model.Finding
	var ttl *types.TimeToLiveDescription
	to, err := a.client.DescribeTimeToLive(ctx, &awsddb.DescribeTimeToLiveInput{TableName: aws.String(ref.ID)})
	if err != nil {
		findings = append(findings, finding(ref.ID, "DYNAMODB_TTL_UNAVAILABLE", err))
	} else {
		ttl = to.TimeToLiveDescription
	}
	var tags []types.Tag
	if d.Table.TableArn != nil {
		var token *string
		for {
			o, e := a.client.ListTagsOfResource(ctx, &awsddb.ListTagsOfResourceInput{ResourceArn: d.Table.TableArn, NextToken: token})
			if e != nil {
				findings = append(findings, finding(ref.ID, "DYNAMODB_TAGS_UNAVAILABLE", e))
				break
			}
			tags = append(tags, o.Tags...)
			token = o.NextToken
			if token == nil || *token == "" {
				break
			}
		}
	}
	structure := Normalize(d.Table, ttl, tags, opts.PreserveProvisioned)
	snapshot, err := model.NewSnapshot(ref, "dynamodb", structure)
	if err != nil {
		return nil, err
	}
	snapshot.Findings = findings
	if opts.IncludeData {
		opts.EstimatedRecords = aws.ToInt64(d.Table.ItemCount)
		opts.EstimatedBytes = aws.ToInt64(d.Table.TableSizeBytes)
		result, captureErr := a.captureData(ctx, ref.ID, opts, directoryWriter{root: opts.ArtifactDirectory})
		if captureErr != nil {
			if !opts.AllowPartialData || errors.Is(captureErr, context.Canceled) || errors.Is(captureErr, context.DeadlineExceeded) {
				return nil, captureErr
			}
			snapshot.Findings = append(snapshot.Findings, model.Finding{Code: "DATA_CAPTURE_PARTIAL", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: ref.ID, Property: "data", Message: "Fixture capture was incomplete: " + captureErr.Error(), Remediation: "Rerun the capture after restoring read access, or disable allow_partial_data."})
		} else {
			snapshot.Dataset = &result.Dataset
			if result.Truncated {
				snapshot.Findings = append(snapshot.Findings, model.Finding{Code: "DYNAMODB_DATA_LIMIT_REACHED", Severity: model.SeverityInfo, Support: model.SupportPartial, Resource: ref.ID, Message: "The configured fixture boundary was reached."})
			}
		}
	}
	return snapshot, nil
}

// CaptureReusable deliberately never reuses completed DynamoDB datasets. A
// scan cursor proves only interrupted-run progress, not that a prior completed
// scan still reflects the table's current contents.
func (a *Adapter) CaptureReusable(ctx context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions, request catalog.ReuseRequest) (catalog.ReuseResult, error) {
	structureOptions := opts
	structureOptions.IncludeData = false
	snapshot, err := a.Capture(ctx, scope, ref, structureOptions)
	if err != nil || !opts.IncludeData {
		return catalog.ReuseResult{Snapshot: snapshot}, err
	}
	definition, err := dynamoCaptureDefinition(scope, ref, opts)
	if err != nil {
		return catalog.ReuseResult{}, err
	}
	reason := dynamoRefreshReason(definition, request)
	result, err := a.captureData(ctx, ref.ID, opts, directoryWriter{root: opts.ArtifactDirectory})
	if err != nil {
		if !opts.AllowPartialData || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return catalog.ReuseResult{}, err
		}
		snapshot.Findings = append(snapshot.Findings, model.Finding{Code: "DATA_CAPTURE_PARTIAL", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: ref.ID, Property: "data", Message: "Fixture capture was incomplete: " + err.Error(), Remediation: "Rerun the capture after restoring read access, or disable allow_partial_data."})
		return catalog.ReuseResult{Snapshot: snapshot}, nil
	}
	snapshot.Dataset = &result.Dataset
	if result.Truncated {
		snapshot.Findings = append(snapshot.Findings, model.Finding{Code: "DYNAMODB_DATA_LIMIT_REACHED", Severity: model.SeverityInfo, Support: model.SupportPartial, Resource: ref.ID, Message: "The configured fixture boundary was reached."})
	}
	return catalog.ReuseResult{Snapshot: snapshot, Resource: dynamoLedgerResource(ref, definition, result.Dataset, reason)}, nil
}

func dynamoRefreshReason(definition string, request catalog.ReuseRequest) captureledger.Reason {
	if request.InvalidationReason == captureledger.ReasonFormatChanged {
		return captureledger.ReasonFormatChanged
	}
	matching := request.Candidate
	if matching != nil && matching.CaptureDefinition != definition {
		matching = nil
	}
	if matching == nil {
		if request.Candidate != nil {
			return captureledger.ReasonCaptureDefinitionChanged
		}
		if request.InvalidationReason != "" {
			return request.InvalidationReason
		}
		return captureledger.ReasonNoCandidate
	}
	for _, unit := range matching.Units {
		if unit.Outcome == captureledger.UnitOutcomeInvalidated && unit.Reason != "" && unit.Reason != captureledger.ReasonReused {
			return unit.Reason
		}
		if request.Validate != nil {
			for _, artifact := range unit.Artifacts {
				if err := request.Validate(artifact); err != nil {
					if reason, ok := captureledger.InvalidationReason(err); ok {
						return reason
					}
					return captureledger.ReasonArtifactCorrupt
				}
			}
		}
	}
	if request.InvalidationReason != "" {
		return request.InvalidationReason
	}
	return captureledger.ReasonFreshnessUnproven
}
func (a *Adapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }
func (a *Adapter) Validate(s *model.Snapshot, _ model.Capabilities) []model.Finding {
	t, err := model.DecodeStructure[Table](s)
	if err != nil {
		return nil
	}
	var out []model.Finding
	for _, idx := range t.LocalIndexes {
		if idx.Projection.Type == "INCLUDE" {
			out = append(out, model.Finding{Code: "FLOCI_DYNAMODB_LSI_INCLUDE_READBACK", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: t.Name, Property: "local_secondary_indexes.projection", Message: "Floci 1.6.0 does not faithfully report INCLUDE attributes for local secondary indexes."})
		}
	}
	return out
}
