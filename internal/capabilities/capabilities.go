package capabilities

import (
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

const SchemaVersion = 1

type Report struct {
	SchemaVersion           int                 `json:"schema_version"`
	ToolVersion             string              `json:"tool_version"`
	FlociVersion            string              `json:"floci_version"`
	ManifestSchemas         []int               `json:"manifest_schemas"`
	Services                []ServiceCapability `json:"services"`
	CompatibilityCommitment string              `json:"compatibility_commitment"`
}

type ServiceCapability struct {
	Service   string   `json:"service"`
	Support   string   `json:"support"`
	DataModes []string `json:"data_modes"`
}

func Current(toolVersion string) Report {
	if toolVersion == "" {
		toolVersion = "dev"
	}
	manifestSchemas := make([]int, 0, model.CurrentManifestSchemaVersion-model.MinimumManifestSchemaVersion+1)
	for schema := model.MinimumManifestSchemaVersion; schema <= model.CurrentManifestSchemaVersion; schema++ {
		manifestSchemas = append(manifestSchemas, schema)
	}
	return Report{
		SchemaVersion: SchemaVersion, ToolVersion: toolVersion, FlociVersion: config.DefaultFlociVersion,
		ManifestSchemas:         manifestSchemas,
		CompatibilityCommitment: "pre-1.0; schema and CLI contracts may evolve with release notes",
		Services:                serviceCapabilities(),
	}
}

func serviceCapabilities() []ServiceCapability {
	facts := model.SupportedServiceFacts()
	out := make([]ServiceCapability, 0, len(facts))
	for _, fact := range facts {
		out = append(out, ServiceCapability{Service: fact.Name, Support: string(fact.Support), DataModes: append([]string(nil), fact.DataModes...)})
	}
	return out
}
