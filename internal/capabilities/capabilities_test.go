package capabilities

import (
	"reflect"
	"testing"
)

func TestCurrentIsStableAndSorted(t *testing.T) {
	one := Current("")
	two := Current("v0.11.0")
	if one.SchemaVersion != 1 || one.ToolVersion != "dev" || len(one.Services) != 9 {
		t.Fatalf("report = %#v", one)
	}
	if !reflect.DeepEqual(one.Services[0].Service, "dynamodb") || one.ManifestSchemas[0] != 1 || one.ManifestSchemas[len(one.ManifestSchemas)-1] != 3 {
		t.Fatalf("report ordering = %#v", one)
	}
	if two.ToolVersion != "v0.11.0" {
		t.Fatalf("tool version = %q", two.ToolVersion)
	}
}
