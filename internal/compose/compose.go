package compose

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nkootstra/floceed/internal/config"
	"go.yaml.in/yaml/v3"
)

const Image = "floci/floci:1.6.0-compat@sha256:15ba10dace4a29d94f0e36c03dc3c2ec5bfc4364a1cf9c67f9def4e530ae2c2c"

var volumeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// The Compose document model. Render emits exactly one floci service that
// mounts only generated files; structs keep the output deterministic and
// free of hand-built YAML.
type composeProject struct {
	Services map[string]service `yaml:"services"`
	Volumes  map[string]any     `yaml:"volumes,omitempty"`
}

type service struct {
	Image       string      `yaml:"image"`
	Environment environment `yaml:"environment"`
	Ports       []string    `yaml:"ports"`
	Volumes     []mount     `yaml:"volumes"`
}

type environment struct {
	HookTimeout string `yaml:"FLOCI_INIT_HOOKS_TIMEOUT_SECONDS"`
	StorageMode string `yaml:"FLOCI_STORAGE_MODE,omitempty"`
	StoragePath string `yaml:"FLOCI_STORAGE_PERSISTENT_PATH,omitempty"`
}

type mount struct {
	Type     string       `yaml:"type"`
	Source   string       `yaml:"source"`
	Target   string       `yaml:"target"`
	ReadOnly bool         `yaml:"read_only,omitempty"`
	Bind     *bindOptions `yaml:"bind,omitempty"`
}

type bindOptions struct {
	CreateHostPath bool `yaml:"create_host_path"`
}

// Render returns a deterministic Compose project which mounts only generated files.
func Render(project config.Project) ([]byte, error) {
	if project.Target.FlociVersion != config.DefaultFlociVersion {
		return nil, fmt.Errorf("unsupported Floci version %q", project.Target.FlociVersion)
	}
	env := environment{HookTimeout: fmt.Sprint(project.Target.HookTimeoutSeconds)}
	mounts := []mount{
		{Type: "bind", Source: "./init/ready.d", Target: "/etc/floci/init/ready.d", ReadOnly: true, Bind: &bindOptions{CreateHostPath: false}},
		{Type: "bind", Source: "./", Target: "/floceed", ReadOnly: true, Bind: &bindOptions{CreateHostPath: false}},
	}
	var volumes map[string]any
	if p := project.Target.Persistence; p.Enabled {
		env.StorageMode = "persistent"
		env.StoragePath = "/app/data"
		if p.Path != "" {
			if filepath.IsAbs(p.Path) || strings.Contains(filepath.ToSlash(filepath.Clean(p.Path)), "..") {
				return nil, fmt.Errorf("persistence path must be a safe relative path")
			}
			mounts = append(mounts, mount{Type: "bind", Source: "../" + filepath.ToSlash(filepath.Clean(p.Path)), Target: "/app/data", Bind: &bindOptions{CreateHostPath: true}})
		} else {
			name := p.Volume
			if name == "" {
				name = "floceed-data"
			}
			if !volumeName.MatchString(name) {
				return nil, fmt.Errorf("invalid persistence volume %q", name)
			}
			mounts = append(mounts, mount{Type: "volume", Source: name, Target: "/app/data"})
			volumes = map[string]any{name: struct{}{}}
		}
	}
	doc := composeProject{Services: map[string]service{"floci": {
		Image:       Image,
		Environment: env,
		Ports:       []string{fmt.Sprintf("127.0.0.1:%d:4566", project.Target.Port)},
		Volumes:     mounts,
	}}, Volumes: volumes}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, fmt.Errorf("encode Compose project: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("encode Compose project: %w", err)
	}
	return buf.Bytes(), nil
}
