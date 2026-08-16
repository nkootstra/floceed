package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nkootstra/floceed/internal/governance"
)

type compiledDynamoRule struct {
	rule  governance.Rule
	parts []string
}

func compileDynamoRules(rules []governance.Rule) []compiledDynamoRule {
	compiled := make([]compiledDynamoRule, 0, len(rules))
	for _, rule := range rules {
		parts := strings.Split(rule.Target.Path, ".")
		if rule.Service == governance.ServiceDynamoDB && rule.Target.Kind == governance.TargetDynamoDBAttribute && len(parts) != 0 {
			compiled = append(compiled, compiledDynamoRule{rule: rule, parts: parts})
		}
	}
	return compiled
}

// CanonicalItem converts AttributeValues to DynamoDB JSON with stable map and set ordering.
func CanonicalItem(item map[string]types.AttributeValue) ([]byte, error) {
	v, err := attributeMap(item)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// GovernItem applies exact DynamoDB attribute rules to a private copy. Source
// values remain in memory only; callers receive a value safe for durable use.
func GovernItem(item map[string]types.AttributeValue, rules []governance.Rule, engine *governance.Engine, audit *governance.Audit) (map[string]types.AttributeValue, error) {
	return governItemCompiled(item, compileDynamoRules(rules), engine, audit)
}

func governItemCompiled(item map[string]types.AttributeValue, rules []compiledDynamoRule, engine *governance.Engine, audit *governance.Audit) (map[string]types.AttributeValue, error) {
	out := cloneAttributeMapShallow(item)
	for _, compiled := range rules {
		rule, parts := compiled.rule, compiled.parts
		container := out
		for _, part := range parts[:len(parts)-1] {
			member, ok := container[part].(*types.AttributeValueMemberM)
			if !ok {
				container = nil
				break
			}
			cloned := cloneAttributeMapShallow(member.Value)
			container[part] = &types.AttributeValueMemberM{Value: cloned}
			container = cloned
		}
		if container == nil {
			continue
		}
		name := parts[len(parts)-1]
		value, ok := container[name]
		if !ok {
			continue
		}
		source, err := scalarBytes(value)
		if err != nil {
			return nil, fmt.Errorf("apply DynamoDB governance rule: unsupported attribute type")
		}
		result, err := engine.Apply(rule, source)
		if err != nil {
			return nil, fmt.Errorf("apply DynamoDB governance rule: %w", err)
		}
		if result.Omit {
			delete(container, name)
			recordRuleAudit(audit, rule.ID)
			continue
		}
		container[name], err = transformedScalar(value, result.Value)
		if err != nil {
			return nil, fmt.Errorf("apply DynamoDB governance rule: incompatible replacement")
		}
		recordRuleAudit(audit, rule.ID)
	}
	return out, nil
}

func recordRuleAudit(audit *governance.Audit, ruleID string) {
	if audit != nil {
		audit.Record(ruleID)
	}
}

func cloneAttributeMapShallow(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(item))
	for key, value := range item {
		out[key] = value
	}
	return out
}

func scalarBytes(value types.AttributeValue) ([]byte, error) {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return []byte(value.Value), nil
	case *types.AttributeValueMemberN:
		return []byte(value.Value), nil
	case *types.AttributeValueMemberB:
		return append([]byte(nil), value.Value...), nil
	case *types.AttributeValueMemberBOOL:
		return []byte(fmt.Sprintf("%t", value.Value)), nil
	case *types.AttributeValueMemberNULL:
		return []byte(fmt.Sprintf("%t", value.Value)), nil
	default:
		return nil, fmt.Errorf("not a scalar")
	}
}

func transformedScalar(original types.AttributeValue, transformed []byte) (types.AttributeValue, error) {
	switch original.(type) {
	case *types.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: string(transformed)}, nil
	case *types.AttributeValueMemberB:
		return &types.AttributeValueMemberB{Value: append([]byte(nil), transformed...)}, nil
	default:
		return nil, fmt.Errorf("transformation would change scalar type")
	}
}

func attributeAtPath(item map[string]types.AttributeValue, path string) (types.AttributeValue, bool) {
	parts := strings.Split(path, ".")
	container := item
	for _, part := range parts[:len(parts)-1] {
		member, ok := container[part].(*types.AttributeValueMemberM)
		if !ok {
			return nil, false
		}
		container = member.Value
	}
	value, ok := container[parts[len(parts)-1]]
	return value, ok
}

func cohortKeyValues(item map[string]types.AttributeValue, cohort governance.Cohort) ([][]byte, bool, error) {
	for _, predicate := range cohort.Predicates {
		value, ok := attributeAtPath(item, predicate.Attribute)
		if !ok || !scalarMatches(value, predicate.Value) {
			return nil, false, nil
		}
	}
	values := make([][]byte, len(cohort.KeyPaths))
	for i, path := range cohort.KeyPaths {
		value, ok := attributeAtPath(item, path)
		if !ok {
			return nil, false, nil
		}
		canonical, err := attribute(value)
		if err != nil {
			return nil, false, fmt.Errorf("rank DynamoDB cohort: unsupported key attribute")
		}
		values[i], err = json.Marshal(canonical)
		if err != nil {
			return nil, false, fmt.Errorf("rank DynamoDB cohort: encode key attribute")
		}
	}
	return values, true, nil
}

func scalarMatches(value types.AttributeValue, expected any) bool {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		expected, ok := expected.(string)
		return ok && value.Value == expected
	case *types.AttributeValueMemberN:
		return value.Value == fmt.Sprint(expected)
	case *types.AttributeValueMemberBOOL:
		expected, ok := expected.(bool)
		return ok && value.Value == expected
	case *types.AttributeValueMemberNULL:
		expected, ok := expected.(bool)
		return ok && value.Value == expected
	default:
		return false
	}
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
