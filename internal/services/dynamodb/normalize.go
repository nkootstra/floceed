package dynamodb

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func Normalize(d *types.TableDescription, ttl *types.TimeToLiveDescription, tags []types.Tag, preserveProvisioned bool) Table {
	t := Table{Name: aws.ToString(d.TableName), BillingMode: "PAY_PER_REQUEST", SourceBillingMode: string(types.BillingModePayPerRequest)}
	if d.BillingModeSummary != nil && d.BillingModeSummary.BillingMode != "" {
		t.SourceBillingMode = string(d.BillingModeSummary.BillingMode)
	} else if d.ProvisionedThroughput != nil {
		t.SourceBillingMode = string(types.BillingModeProvisioned)
	}
	if preserveProvisioned && t.SourceBillingMode == string(types.BillingModeProvisioned) {
		t.BillingMode = t.SourceBillingMode
		t.ReadCapacity = aws.ToInt64(d.ProvisionedThroughput.ReadCapacityUnits)
		t.WriteCapacity = aws.ToInt64(d.ProvisionedThroughput.WriteCapacityUnits)
	}
	for _, v := range d.AttributeDefinitions {
		t.Attributes = append(t.Attributes, AttributeDefinition{aws.ToString(v.AttributeName), string(v.AttributeType)})
	}
	for _, v := range d.KeySchema {
		t.Keys = append(t.Keys, KeyElement{aws.ToString(v.AttributeName), string(v.KeyType)})
	}
	for _, v := range d.GlobalSecondaryIndexes {
		t.GlobalIndexes = append(t.GlobalIndexes, index(v.IndexName, v.KeySchema, v.Projection, v.ProvisionedThroughput))
	}
	for _, v := range d.LocalSecondaryIndexes {
		t.LocalIndexes = append(t.LocalIndexes, index(v.IndexName, v.KeySchema, v.Projection, nil))
	}
	if d.StreamSpecification != nil {
		t.Stream = Stream{aws.ToBool(d.StreamSpecification.StreamEnabled), string(d.StreamSpecification.StreamViewType)}
	}
	if ttl != nil {
		t.TTL = TTL{ttl.TimeToLiveStatus == types.TimeToLiveStatusEnabled || ttl.TimeToLiveStatus == types.TimeToLiveStatusEnabling, aws.ToString(ttl.AttributeName)}
	}
	for _, v := range tags {
		t.Tags = append(t.Tags, Tag{aws.ToString(v.Key), aws.ToString(v.Value)})
	}
	sort.Slice(t.Attributes, func(i, j int) bool { return t.Attributes[i].Name < t.Attributes[j].Name })
	sort.Slice(t.GlobalIndexes, func(i, j int) bool { return t.GlobalIndexes[i].Name < t.GlobalIndexes[j].Name })
	sort.Slice(t.LocalIndexes, func(i, j int) bool { return t.LocalIndexes[i].Name < t.LocalIndexes[j].Name })
	sort.Slice(t.Tags, func(i, j int) bool {
		if t.Tags[i].Key == t.Tags[j].Key {
			return t.Tags[i].Value < t.Tags[j].Value
		}
		return t.Tags[i].Key < t.Tags[j].Key
	})
	return t
}

func index(name *string, keys []types.KeySchemaElement, p *types.Projection, throughput *types.ProvisionedThroughputDescription) SecondaryIndex {
	v := SecondaryIndex{Name: aws.ToString(name)}
	if p != nil {
		v.Projection = Projection{Type: string(p.ProjectionType), NonKeyAttributes: append([]string(nil), p.NonKeyAttributes...)}
	}
	for _, k := range keys {
		v.Keys = append(v.Keys, KeyElement{aws.ToString(k.AttributeName), string(k.KeyType)})
	}
	sort.Strings(v.Projection.NonKeyAttributes)
	if throughput != nil {
		v.ReadCapacity = aws.ToInt64(throughput.ReadCapacityUnits)
		v.WriteCapacity = aws.ToInt64(throughput.WriteCapacityUnits)
	}
	return v
}
