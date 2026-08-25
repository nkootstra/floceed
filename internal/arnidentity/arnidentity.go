// Package arnidentity centralizes the structural parts of AWS ARN identity
// checks shared by configuration and decoded-manifest validation.
package arnidentity

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

type Identity struct {
	Partition string
	Service   string
	Region    string
	Account   string
	Resource  string
}

func Parse(value string) (Identity, error) {
	parsed, err := arn.Parse(value)
	if err != nil || parsed.Partition == "" || parsed.Service == "" || parsed.Resource == "" {
		return Identity{}, fmt.Errorf("invalid ARN %q", value)
	}
	return Identity{Partition: parsed.Partition, Service: parsed.Service, Region: parsed.Region, Account: parsed.AccountID, Resource: parsed.Resource}, nil
}

func (id Identity) ResourceName(prefix string) (string, bool) {
	if !strings.HasPrefix(id.Resource, prefix) {
		return "", false
	}
	return strings.TrimPrefix(id.Resource, prefix), true
}
