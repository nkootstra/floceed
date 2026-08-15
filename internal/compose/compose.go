package compose

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nkootstra/floceed/internal/config"
)

const Image = "floci/floci:1.6.0-compat@sha256:15ba10dace4a29d94f0e36c03dc3c2ec5bfc4364a1cf9c67f9def4e530ae2c2c"

var volumeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// Render returns a deterministic Compose project which mounts only generated files.
func Render(project config.Project) ([]byte, error) {
	if project.Target.FlociVersion != config.DefaultFlociVersion {
		return nil, fmt.Errorf("unsupported Floci version %q", project.Target.FlociVersion)
	}
	var b strings.Builder
	b.WriteString("services:\n  floci:\n    image: \"")
	b.WriteString(Image)
	b.WriteString("\"\n    environment:\n      FLOCI_INIT_HOOKS_TIMEOUT_SECONDS: \"")
	b.WriteString(fmt.Sprint(project.Target.HookTimeoutSeconds))
	b.WriteString("\"\n")
	if project.Target.Persistence.Enabled {
		b.WriteString("      FLOCI_STORAGE_MODE: \"persistent\"\n      FLOCI_STORAGE_PERSISTENT_PATH: \"/app/data\"\n")
	}
	b.WriteString("    ports:\n      - \"")
	b.WriteString(fmt.Sprintf("127.0.0.1:%d:4566", project.Target.Port))
	b.WriteString("\"\n    volumes:\n")
	for _, mount := range []struct{ source, target string }{
		{"./init/ready.d", "/etc/floci/init/ready.d"},
		{"./", "/floceed"},
	} {
		b.WriteString("      - type: bind\n        source: ")
		b.WriteString(mount.source)
		b.WriteString("\n        target: ")
		b.WriteString(mount.target)
		b.WriteString("\n        read_only: true\n        bind:\n          create_host_path: false\n")
	}
	if p := project.Target.Persistence; p.Enabled {
		if p.Path != "" {
			if filepath.IsAbs(p.Path) || strings.Contains(filepath.ToSlash(filepath.Clean(p.Path)), "..") {
				return nil, fmt.Errorf("persistence path must be a safe relative path")
			}
			b.WriteString("      - type: bind\n        source: ../")
			b.WriteString(filepath.ToSlash(filepath.Clean(p.Path)))
			b.WriteString("\n        target: /app/data\n        bind:\n          create_host_path: true\n")
		} else {
			name := p.Volume
			if name == "" {
				name = "floceed-data"
			}
			if !volumeName.MatchString(name) {
				return nil, fmt.Errorf("invalid persistence volume %q", name)
			}
			b.WriteString("      - type: volume\n        source: ")
			b.WriteString(name)
			b.WriteString("\n        target: /app/data\nvolumes:\n  ")
			b.WriteString(name)
			b.WriteString(": {}\n")
		}
	}
	return []byte(b.String()), nil
}
