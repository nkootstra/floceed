package testfixture

import (
	"fmt"
	"slices"

	"github.com/nkootstra/floceed/internal/bundle"
)

type iamPolicyDocument struct {
	Version   string         `json:"Version"`
	Statement []iamStatement `json:"Statement"`
}

type iamStatement struct {
	Sid      string   `json:"Sid"`
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource string   `json:"Resource"`
}

var representativeServiceDiscoveryIAMActions = []string{
	"apigateway:GET",
	"dynamodb:ListTables",
	"events:ListEventBuses",
	"kinesis:ListStreams",
	"lambda:ListFunctions",
	"logs:DescribeLogGroups",
	"s3:ListAllMyBuckets",
	"secretsmanager:ListSecrets",
	"sns:ListTopics",
	"sqs:ListQueues",
	"ssm:DescribeParameters",
	"states:ListStateMachines",
}

// RepresentativeServiceDiscoveryIAMActions returns the read-only actions used
// by the integration guard to probe every supported replay service.
func RepresentativeServiceDiscoveryIAMActions() []string {
	return slices.Clone(representativeServiceDiscoveryIAMActions)
}

// RepresentativeAppIAMPolicy returns a deterministic identity-based IAM
// policy document for a fictional application execution role scoped to the
// representative account and region. It grants the read-only operations used
// to probe every supported replay service, plus scoped object and item reads.
// It deliberately omits scan and write actions so enforced local replays can
// exercise AccessDenied paths.
func RepresentativeAppIAMPolicy(bucket string) []byte {
	document := iamPolicyDocument{
		Version: "2012-10-17",
		Statement: []iamStatement{
			{
				Sid:    "RepresentativeTableRead",
				Effect: "Allow",
				Action: []string{"dynamodb:DescribeTable", "dynamodb:GetItem"},
				// Floci 1.6.0 resolves DynamoDB data-plane resources at
				// table-wildcard granularity and never matches a concrete
				// table name, so the least-privilege document must use it.
				Resource: fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/*", representativeRegion, representativeAccount),
			},
			{
				Sid:      "RepresentativeObjectRead",
				Effect:   "Allow",
				Action:   []string{"s3:GetObject"},
				Resource: fmt.Sprintf("arn:aws:s3:::%s/*", bucket),
			},
			{
				Sid:      "SupportedServiceDiscovery",
				Effect:   "Allow",
				Action:   RepresentativeServiceDiscoveryIAMActions(),
				Resource: "*",
			},
		},
	}
	data, err := bundle.CanonicalJSON(document)
	if err != nil {
		panic(err)
	}
	return data
}
