package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CanonicalItem converts AttributeValues to DynamoDB JSON with stable map and set ordering.
func CanonicalItem(item map[string]types.AttributeValue) ([]byte, error) {
	v, err := attributeMap(item)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func attributeMap(m map[string]types.AttributeValue) (map[string]any, error) {
	out := make(map[string]any, len(m))
	for k, v := range m {
		x, err := attribute(v)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = x
	}
	return out, nil
}

func attribute(v types.AttributeValue) (map[string]any, error) {
	switch x := v.(type) {
	case *types.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(x.Value)}, nil
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": x.Value}, nil
	case *types.AttributeValueMemberBS:
		a := make([]string, len(x.Value))
		for i, b := range x.Value {
			a[i] = base64.StdEncoding.EncodeToString(b)
		}
		sort.Strings(a)
		return map[string]any{"BS": a}, nil
	case *types.AttributeValueMemberL:
		a := make([]any, len(x.Value))
		for i, e := range x.Value {
			z, err := attribute(e)
			if err != nil {
				return nil, err
			}
			a[i] = z
		}
		return map[string]any{"L": a}, nil
	case *types.AttributeValueMemberM:
		m, err := attributeMap(x.Value)
		return map[string]any{"M": m}, err
	case *types.AttributeValueMemberN:
		return map[string]any{"N": x.Value}, nil
	case *types.AttributeValueMemberNS:
		a := append([]string(nil), x.Value...)
		sort.Strings(a)
		return map[string]any{"NS": a}, nil
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": x.Value}, nil
	case *types.AttributeValueMemberS:
		return map[string]any{"S": x.Value}, nil
	case *types.AttributeValueMemberSS:
		a := append([]string(nil), x.Value...)
		sort.Strings(a)
		return map[string]any{"SS": a}, nil
	default:
		return nil, fmt.Errorf("unsupported AttributeValue %T", v)
	}
}
