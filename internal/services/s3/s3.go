// Package s3 implements read-only capture of Amazon S3 buckets.
package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

// Client is deliberately source/read-only. Replay uses boto3 against Floci.
type Client interface {
	ListBuckets(context.Context, *awss3.ListBucketsInput, ...func(*awss3.Options)) (*awss3.ListBucketsOutput, error)
	HeadBucket(context.Context, *awss3.HeadBucketInput, ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error)
	GetBucketTagging(context.Context, *awss3.GetBucketTaggingInput, ...func(*awss3.Options)) (*awss3.GetBucketTaggingOutput, error)
	GetBucketVersioning(context.Context, *awss3.GetBucketVersioningInput, ...func(*awss3.Options)) (*awss3.GetBucketVersioningOutput, error)
	GetBucketCors(context.Context, *awss3.GetBucketCorsInput, ...func(*awss3.Options)) (*awss3.GetBucketCorsOutput, error)
	GetBucketLifecycleConfiguration(context.Context, *awss3.GetBucketLifecycleConfigurationInput, ...func(*awss3.Options)) (*awss3.GetBucketLifecycleConfigurationOutput, error)
	GetBucketEncryption(context.Context, *awss3.GetBucketEncryptionInput, ...func(*awss3.Options)) (*awss3.GetBucketEncryptionOutput, error)
	GetBucketPolicy(context.Context, *awss3.GetBucketPolicyInput, ...func(*awss3.Options)) (*awss3.GetBucketPolicyOutput, error)
	GetBucketWebsite(context.Context, *awss3.GetBucketWebsiteInput, ...func(*awss3.Options)) (*awss3.GetBucketWebsiteOutput, error)
	GetPublicAccessBlock(context.Context, *awss3.GetPublicAccessBlockInput, ...func(*awss3.Options)) (*awss3.GetPublicAccessBlockOutput, error)
	GetObjectLockConfiguration(context.Context, *awss3.GetObjectLockConfigurationInput, ...func(*awss3.Options)) (*awss3.GetObjectLockConfigurationOutput, error)
	GetBucketNotificationConfiguration(context.Context, *awss3.GetBucketNotificationConfigurationInput, ...func(*awss3.Options)) (*awss3.GetBucketNotificationConfigurationOutput, error)
	GetBucketReplication(context.Context, *awss3.GetBucketReplicationInput, ...func(*awss3.Options)) (*awss3.GetBucketReplicationOutput, error)
	GetBucketLogging(context.Context, *awss3.GetBucketLoggingInput, ...func(*awss3.Options)) (*awss3.GetBucketLoggingOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	GetObjectTagging(context.Context, *awss3.GetObjectTaggingInput, ...func(*awss3.Options)) (*awss3.GetObjectTaggingOutput, error)
}

type Adapter struct {
	client Client
	names  map[string]struct{}
}

func New(client Client) *Adapter { return &Adapter{client: client} }

// NewFiltered restricts discovery to exact bucket names. Capture still requires an
// explicit ResourceRef and is therefore unaffected by this discovery filter.
func NewFiltered(client Client, names []string) *Adapter {
	a := New(client)
	if len(names) != 0 {
		a.names = make(map[string]struct{}, len(names))
		for _, name := range names {
			a.names[name] = struct{}{}
		}
	}
	return a
}
func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "s3", DisplayName: "S3", Support: model.SupportPartial}
}

func (*Adapter) Plan(project config.Project, includeData bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{}
	if len(project.Resources.S3) == 0 {
		return contribution
	}
	contribution.RequiredIAMActions = []string{"s3:ListAllMyBuckets", "s3:GetBucketLocation", "s3:ListBucket", "s3:GetBucketTagging", "s3:GetBucketVersioning", "s3:GetBucketCORS", "s3:GetLifecycleConfiguration", "s3:GetEncryptionConfiguration", "s3:GetBucketPolicy", "s3:GetBucketWebsite", "s3:GetBucketPublicAccessBlock", "s3:GetObjectLockConfiguration", "s3:GetBucketNotification", "s3:GetReplicationConfiguration", "s3:GetBucketLogging"}
	for _, resource := range project.Resources.S3 {
		options := model.CaptureOptions{}
		if resource.Data != nil {
			options.IncludeData = resource.Data.Enabled
			options.Mode = string(resource.Data.Mode)
			if options.Mode == "" {
				options.Mode = string(config.DataModeBounded)
			}
			options.Prefixes = resource.Data.Prefixes
			options.Overwrite = string(resource.Data.Overwrite)
			options.Limits = model.DataLimits{MaxObjects: resource.Data.MaxObjects, MaxObjectBytes: resource.Data.MaxObjectBytes, MaxTotalBytes: resource.Data.MaxTotalBytes}
			if includeData && resource.Data.Enabled {
				contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "s3:GetObject", "s3:GetObjectTagging")
			}
		}
		contribution.Selections = append(contribution.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: resource.Name, ARN: "arn:aws:s3:::" + resource.Name}, Options: options})
	}
	return contribution
}

func (*Adapter) FinalizePlanning(snapshot *model.Snapshot, dependencies []model.Dependency) ([]model.Finding, error) {
	if len(dependencies) == 0 {
		return nil, nil
	}
	bucket, err := model.DecodeStructure[Bucket](snapshot)
	if err != nil {
		return nil, err
	}
	bucket.Notifications = nil
	if err := model.SetStructure(snapshot, bucket); err != nil {
		return nil, err
	}
	findings := make([]model.Finding, 0, len(dependencies))
	for _, dependency := range dependencies {
		findings = append(findings, model.Finding{Code: "DEPENDENCY_NOT_SELECTED", Severity: model.SeverityWarning, Support: model.SupportImporterUnsupported, Resource: snapshot.Resource.ID, Property: dependency.Kind, Message: fmt.Sprintf("Reference to %s %s is disabled because that resource type is not part of the MVP", dependency.To.Service, dependency.To.ID), Remediation: "Remove the source link or add it after a future floceed adapter supports the dependency."})
	}
	return findings, nil
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type Object struct {
	Key             string            `json:"key"`
	Path            string            `json:"path"`
	Size            int64             `json:"size"`
	SHA256          string            `json:"sha256"`
	ETag            string            `json:"etag,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
	CacheControl    string            `json:"cache_control,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Tags            []Tag             `json:"tags,omitempty"`
	Checksums       map[string]string `json:"checksums,omitempty"`
	Overwrite       string            `json:"overwrite,omitempty"`
}
type Bucket struct {
	Name              string   `json:"name"`
	Region            string   `json:"region"`
	Tags              []Tag    `json:"tags,omitempty"`
	Versioning        string   `json:"versioning,omitempty"`
	CORS              any      `json:"cors,omitempty"`
	Lifecycle         any      `json:"lifecycle,omitempty"`
	Encryption        any      `json:"encryption,omitempty"`
	Policy            string   `json:"policy,omitempty"`
	Website           any      `json:"website,omitempty"`
	PublicAccessBlock any      `json:"public_access_block,omitempty"`
	ObjectLock        any      `json:"object_lock,omitempty"`
	Notifications     any      `json:"notifications,omitempty"`
	Objects           []Object `json:"objects,omitempty"`
}

func (a *Adapter) Discover(ctx context.Context, scope model.SourceScope) (model.DiscoveryResult, error) {
	var buckets []types.Bucket
	paginator := awss3.NewListBucketsPaginator(a.client, &awss3.ListBucketsInput{})
	for paginator.HasMorePages() {
		o, err := paginator.NextPage(ctx)
		if err != nil {
			return model.DiscoveryResult{}, awsconfig.Classify(err, "list S3 buckets", scope.Profile)
		}
		buckets = append(buckets, o.Buckets...)
	}
	sort.Slice(buckets, func(i, j int) bool { return aws.ToString(buckets[i].Name) < aws.ToString(buckets[j].Name) })
	result := model.DiscoveryResult{}
	for _, b := range buckets {
		name, region := aws.ToString(b.Name), aws.ToString(b.BucketRegion)
		if a.names != nil {
			if _, selected := a.names[name]; !selected {
				continue
			}
		}
		var findings []model.Finding
		if region == "" {
			h, err := a.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: b.Name})
			if err != nil {
				findings = append(findings, optionalFinding(name, "S3_REGION_UNAVAILABLE", "region", err))
			} else {
				region = aws.ToString(h.BucketRegion)
			}
		}
		if scope.Region != "" && region != scope.Region {
			continue
		}
		result.Resources = append(result.Resources, model.ResourceSummary{Ref: model.ResourceRef{Service: "s3", Type: "bucket", ID: name, ARN: "arn:aws:s3:::" + name}, Name: name, Region: region, Findings: findings})
	}
	return result, nil
}

func (a *Adapter) Capture(ctx context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	name := ref.ID
	if opts.Overwrite != "" && opts.Overwrite != "if-different" && opts.Overwrite != "always" && opts.Overwrite != "never" {
		return nil, fmt.Errorf("invalid S3 overwrite policy %q", opts.Overwrite)
	}
	h, err := a.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(name)})
	if err != nil {
		return nil, awsconfig.Classify(err, "head S3 bucket "+name, scope.Profile)
	}
	b := Bucket{Name: name, Region: aws.ToString(h.BucketRegion)}
	if b.Region == "" {
		b.Region = scope.Region
	}
	var findings []model.Finding
	appendFinding := func(finding *model.Finding, captureErr error) error {
		if captureErr != nil {
			return captureErr
		}
		if finding != nil {
			findings = append(findings, *finding)
		}
		return nil
	}
	bucket := aws.String(name)
	if f, e := captureOptional(name, "tags", "S3_TAGS_UNAVAILABLE", map[string]bool{"NoSuchTagSet": true}, func() (*awss3.GetBucketTaggingOutput, error) {
		return a.client.GetBucketTagging(ctx, &awss3.GetBucketTaggingInput{Bucket: bucket})
	}, func(v *awss3.GetBucketTaggingOutput) {
		for _, tag := range v.TagSet {
			b.Tags = append(b.Tags, Tag{Key: aws.ToString(tag.Key), Value: aws.ToString(tag.Value)})
		}
		sortTags(b.Tags)
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "versioning", "S3_VERSIONING_UNAVAILABLE", nil, func() (*awss3.GetBucketVersioningOutput, error) {
		return a.client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: bucket})
	}, func(v *awss3.GetBucketVersioningOutput) { b.Versioning = string(v.Status) }); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "cors", "S3_CORS_UNAVAILABLE", map[string]bool{"NoSuchCORSConfiguration": true}, func() (*awss3.GetBucketCorsOutput, error) {
		return a.client.GetBucketCors(ctx, &awss3.GetBucketCorsInput{Bucket: bucket})
	}, func(v *awss3.GetBucketCorsOutput) { b.CORS = normalize(corsShape{CORSRules: v.CORSRules}) }); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "lifecycle", "S3_LIFECYCLE_UNAVAILABLE", map[string]bool{"NoSuchLifecycleConfiguration": true}, func() (*awss3.GetBucketLifecycleConfigurationOutput, error) {
		return a.client.GetBucketLifecycleConfiguration(ctx, &awss3.GetBucketLifecycleConfigurationInput{Bucket: bucket})
	}, func(v *awss3.GetBucketLifecycleConfigurationOutput) {
		b.Lifecycle = normalize(lifecycleShape{Rules: v.Rules, TransitionDefaultMinimumObjectSize: v.TransitionDefaultMinimumObjectSize})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "encryption", "S3_ENCRYPTION_UNAVAILABLE", map[string]bool{"ServerSideEncryptionConfigurationNotFoundError": true}, func() (*awss3.GetBucketEncryptionOutput, error) {
		return a.client.GetBucketEncryption(ctx, &awss3.GetBucketEncryptionInput{Bucket: bucket})
	}, func(v *awss3.GetBucketEncryptionOutput) {
		b.Encryption = normalize(encryptionShape{ServerSideEncryptionConfiguration: v.ServerSideEncryptionConfiguration})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "policy", "S3_POLICY_UNAVAILABLE", map[string]bool{"NoSuchBucketPolicy": true}, func() (*awss3.GetBucketPolicyOutput, error) {
		return a.client.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{Bucket: bucket})
	}, func(v *awss3.GetBucketPolicyOutput) { b.Policy = canonicalJSON(aws.ToString(v.Policy)) }); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "website", "S3_WEBSITE_UNAVAILABLE", map[string]bool{"NoSuchWebsiteConfiguration": true}, func() (*awss3.GetBucketWebsiteOutput, error) {
		return a.client.GetBucketWebsite(ctx, &awss3.GetBucketWebsiteInput{Bucket: bucket})
	}, func(v *awss3.GetBucketWebsiteOutput) {
		b.Website = normalize(websiteShape{ErrorDocument: v.ErrorDocument, IndexDocument: v.IndexDocument, RedirectAllRequestsTo: v.RedirectAllRequestsTo, RoutingRules: v.RoutingRules})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "public_access_block", "S3_PUBLIC_ACCESS_BLOCK_UNAVAILABLE", map[string]bool{"NoSuchPublicAccessBlockConfiguration": true}, func() (*awss3.GetPublicAccessBlockOutput, error) {
		return a.client.GetPublicAccessBlock(ctx, &awss3.GetPublicAccessBlockInput{Bucket: bucket})
	}, func(v *awss3.GetPublicAccessBlockOutput) {
		b.PublicAccessBlock = normalize(publicAccessBlockShape{PublicAccessBlockConfiguration: v.PublicAccessBlockConfiguration})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "object_lock", "S3_OBJECT_LOCK_UNAVAILABLE", map[string]bool{"ObjectLockConfigurationNotFoundError": true}, func() (*awss3.GetObjectLockConfigurationOutput, error) {
		return a.client.GetObjectLockConfiguration(ctx, &awss3.GetObjectLockConfigurationInput{Bucket: bucket})
	}, func(v *awss3.GetObjectLockConfigurationOutput) {
		b.ObjectLock = normalize(objectLockShape{ObjectLockConfiguration: v.ObjectLockConfiguration})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	if f, e := captureOptional(name, "notifications", "S3_NOTIFICATIONS_UNAVAILABLE", nil, func() (*awss3.GetBucketNotificationConfigurationOutput, error) {
		return a.client.GetBucketNotificationConfiguration(ctx, &awss3.GetBucketNotificationConfigurationInput{Bucket: bucket})
	}, func(v *awss3.GetBucketNotificationConfigurationOutput) {
		if v.EventBridgeConfiguration != nil {
			findings = append(findings, unsupported(name, "notifications.eventbridge", "S3_EVENTBRIDGE_NOTIFICATION_UNSUPPORTED", "EventBridge bucket notifications require an EventBridge adapter and are not replayed."))
		}
		b.Notifications = normalize(notificationShape{LambdaFunctionConfigurations: v.LambdaFunctionConfigurations, QueueConfigurations: v.QueueConfigurations, TopicConfigurations: v.TopicConfigurations})
	}); appendFinding(f, e) != nil {
		return nil, e
	}
	// Unsupported settings are probed so their presence is visible, but never replayed.
	if v, e := a.client.GetBucketReplication(ctx, &awss3.GetBucketReplicationInput{Bucket: aws.String(name)}); e == nil && v.ReplicationConfiguration != nil {
		findings = append(findings, unsupported(name, "replication", "S3_REPLICATION_UNSUPPORTED", "Floci 1.6.0 does not support bucket replication."))
	} else if e != nil && !isAbsent(e, map[string]bool{"ReplicationConfigurationNotFoundError": true}) {
		findings = append(findings, optionalFinding(name, "S3_REPLICATION_UNAVAILABLE", "replication", e))
	}
	if v, e := a.client.GetBucketLogging(ctx, &awss3.GetBucketLoggingInput{Bucket: aws.String(name)}); e == nil && v.LoggingEnabled != nil {
		findings = append(findings, unsupported(name, "access_logging", "S3_ACCESS_LOGGING_UNSUPPORTED", "Floci 1.6.0 does not support bucket access logging."))
	} else if e != nil {
		findings = append(findings, optionalFinding(name, "S3_LOGGING_UNAVAILABLE", "access_logging", e))
	}
	s, err := model.NewSnapshot(ref, "s3", b)
	if err != nil {
		return nil, err
	}
	s.Findings = findings
	if opts.IncludeData {
		if opts.ArtifactDirectory == "" {
			return nil, fmt.Errorf("S3 data capture requires an artifact directory")
		}
		if err := a.captureObjects(ctx, name, &b, s, opts); err != nil {
			if !opts.AllowPartialData || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			s.Findings = append(s.Findings, model.Finding{Code: "DATA_CAPTURE_PARTIAL", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: name, Property: "data", Message: "Fixture capture was incomplete: " + err.Error(), Remediation: "Rerun the capture after restoring read access, or disable allow_partial_data."})
		}
		if err := model.SetStructure(s, b); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func captureOptional[T any](resource, property, code string, absent map[string]bool, call func() (T, error), apply func(T)) (*model.Finding, error) {
	value, err := call()
	if err == nil {
		apply(value)
		return nil, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if isAbsent(err, absent) {
		return nil, nil
	}
	finding := optionalFinding(resource, code, property, err)
	return &finding, nil
}

type corsShape struct {
	CORSRules []types.CORSRule `json:"CORSRules,omitempty"`
}
type lifecycleShape struct {
	Rules                              []types.LifecycleRule                    `json:"Rules,omitempty"`
	TransitionDefaultMinimumObjectSize types.TransitionDefaultMinimumObjectSize `json:"TransitionDefaultMinimumObjectSize,omitempty"`
}
type encryptionShape struct {
	ServerSideEncryptionConfiguration *types.ServerSideEncryptionConfiguration `json:"ServerSideEncryptionConfiguration,omitempty"`
}
type websiteShape struct {
	ErrorDocument         *types.ErrorDocument         `json:"ErrorDocument,omitempty"`
	IndexDocument         *types.IndexDocument         `json:"IndexDocument,omitempty"`
	RedirectAllRequestsTo *types.RedirectAllRequestsTo `json:"RedirectAllRequestsTo,omitempty"`
	RoutingRules          []types.RoutingRule          `json:"RoutingRules,omitempty"`
}
type publicAccessBlockShape struct {
	PublicAccessBlockConfiguration *types.PublicAccessBlockConfiguration `json:"PublicAccessBlockConfiguration,omitempty"`
}
type objectLockShape struct {
	ObjectLockConfiguration *types.ObjectLockConfiguration `json:"ObjectLockConfiguration,omitempty"`
}
type notificationShape struct {
	LambdaFunctionConfigurations []types.LambdaFunctionConfiguration `json:"LambdaFunctionConfigurations,omitempty"`
	QueueConfigurations          []types.QueueConfiguration          `json:"QueueConfigurations,omitempty"`
	TopicConfigurations          []types.TopicConfiguration          `json:"TopicConfigurations,omitempty"`
}

func normalize[T any](value T) any {
	data, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("normalize S3 capture shape: %v", err))
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		panic(fmt.Sprintf("normalize S3 capture shape: %v", err))
	}
	return pruneEmpty(normalized)
}

func pruneEmpty(value any) any {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			child = pruneEmpty(child)
			if child == nil || child == "" || child == float64(0) {
				delete(value, key)
			} else if items, ok := child.([]any); ok && len(items) == 0 {
				delete(value, key)
			} else if fields, ok := child.(map[string]any); ok && len(fields) == 0 {
				delete(value, key)
			} else {
				value[key] = child
			}
		}
	case []any:
		for i := range value {
			value[i] = pruneEmpty(value[i])
		}
	}
	return value
}
