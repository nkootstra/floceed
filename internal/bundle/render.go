package bundle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	replayruntime "github.com/nkootstra/floceed/runtime"
)

type ComposeValidator func(context.Context, string) error

type RenderOptions struct {
	ArtifactRoot    string
	ValidateCompose ComposeValidator
}

func Render(ctx context.Context, target string, project config.Project, manifest model.Manifest, opts RenderOptions) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	return WriteAtomic(target, func(stage string) error {
		if err := renderStage(stage, project, manifest, opts.ArtifactRoot); err != nil {
			return err
		}
		if err := ValidateGenerated(stage); err != nil {
			return err
		}
		validate := opts.ValidateCompose
		if validate == nil {
			validate = ValidateCompose
		}
		return validate(ctx, filepath.Join(stage, ComposeFile))
	})
}

func renderStage(stage string, project config.Project, manifest model.Manifest, artifactRoot string) error {
	sortManifest(&manifest)
	files := map[string]struct {
		data []byte
		mode os.FileMode
	}{}
	manifestJSON, err := CanonicalJSON(manifest)
	if err != nil {
		return err
	}
	composeYAML, err := compose.Render(project)
	if err != nil {
		return err
	}
	files["bundle/manifest.json"] = struct {
		data []byte
		mode os.FileMode
	}{manifestJSON, 0o600}
	files[ComposeFile] = struct {
		data []byte
		mode os.FileMode
	}{composeYAML, 0o600}
	files["runtime/replay.py"] = struct {
		data []byte
		mode os.FileMode
	}{replayruntime.ReplayPython, 0o500}
	files["init/ready.d/10-base-resources.py"] = wrapper("base")
	files["init/ready.d/30-resource-links.py"] = wrapper("links")
	files["init/ready.d/60-seed-data.py"] = wrapper("data")
	files[".gitignore"] = struct {
		data []byte
		mode os.FileMode
	}{[]byte("bundle/data/\n"), 0o600}
	for name, file := range files {
		if err := write(stage, name, file.data, file.mode); err != nil {
			return err
		}
	}
	for _, snap := range manifest.Snapshots {
		for _, artifact := range snap.Data {
			if err := ValidateRelativePath(artifact.Path); err != nil {
				return err
			}
			if !strings.HasPrefix(artifact.Path, "bundle/data/") {
				return fmt.Errorf("artifact must be below bundle/data: %s", artifact.Path)
			}
			if artifactRoot == "" {
				return fmt.Errorf("artifact root is required for %s", artifact.Path)
			}
			if err := copyFile(filepath.Join(stage, filepath.FromSlash(artifact.Path)), filepath.Join(artifactRoot, filepath.FromSlash(artifact.Path))); err != nil {
				return err
			}
			copied, err := SumFile(filepath.Join(stage, filepath.FromSlash(artifact.Path)))
			if err != nil {
				return err
			}
			if copied.SHA256 != artifact.SHA256 || copied.Size != artifact.Size {
				return fmt.Errorf("captured artifact checksum mismatch for %s", artifact.Path)
			}
		}
	}
	sums, err := BuildChecksums(stage, "checksums.json")
	if err != nil {
		return err
	}
	b, err := CanonicalJSON(sums)
	if err != nil {
		return err
	}
	return write(stage, "checksums.json", b, 0o600)
}

func wrapper(stage string) struct {
	data []byte
	mode os.FileMode
} {
	return struct {
		data []byte
		mode os.FileMode
	}{[]byte("#!/usr/bin/env python3\nimport runpy, sys\nsys.argv = [\"replay.py\", \"" + stage + "\"]\nrunpy.run_path(\"/floceed/runtime/replay.py\", run_name=\"__main__\")\n"), 0o500}
}

func write(root, name string, data []byte, mode os.FileMode) error {
	if err := ValidateRelativePath(name); err != nil {
		return err
	}
	dst := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}

func copyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open artifact %s: %w", src, err)
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("artifact is not a regular file: %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func sortManifest(m *model.Manifest) {
	sort.Slice(m.Selected, func(i, j int) bool { return refKey(m.Selected[i]) < refKey(m.Selected[j]) })
	sort.Slice(m.Snapshots, func(i, j int) bool { return refKey(m.Snapshots[i].Resource) < refKey(m.Snapshots[j].Resource) })
	for i := range m.Snapshots {
		sort.Slice(m.Snapshots[i].Data, func(a, b int) bool { return m.Snapshots[i].Data[a].Path < m.Snapshots[i].Data[b].Path })
		sortFindings(m.Snapshots[i].Findings)
	}
	sort.Slice(m.Operations, func(i, j int) bool { return m.Operations[i].ID < m.Operations[j].ID })
	for i := range m.Operations {
		sort.Strings(m.Operations[i].DependsOn)
	}
	sortFindings(m.Findings)
}
func refKey(r model.ResourceRef) string { return r.Service + "\x00" + r.Type + "\x00" + r.ID }
func sortFindings(v []model.Finding) {
	sort.Slice(v, func(i, j int) bool {
		a, b := v[i], v[j]
		return a.Code+a.Resource+a.Property < b.Code+b.Resource+b.Property
	})
}

var credentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?:AKIA|ASIA)[0-9A-Z]{16}`),
	regexp.MustCompile(`(?i)[\"']?(aws_secret_access_key|aws_session_token|x-amz-security-token|accesskeyid|secretaccesskey|sessiontoken)[\"']?\s*[:=]\s*[\"']?[^\s\"']{16,}`),
}

func ValidateGenerated(root string) error {
	b, err := os.ReadFile(filepath.Join(root, "checksums.json"))
	if err != nil {
		return err
	}
	var sums Checksums
	if err := json.Unmarshal(b, &sums); err != nil {
		return fmt.Errorf("decode checksums: %w", err)
	}
	if err := VerifyChecksums(root, sums); err != nil {
		return err
	}
	for _, entry := range sums.Files {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(entry.Path)))
		if err != nil {
			return err
		}
		for _, pattern := range credentialPatterns {
			if pattern.Match(data) {
				return fmt.Errorf("potential source credential found in %s", entry.Path)
			}
		}
	}
	return nil
}

func ValidateCompose(ctx context.Context, filename string) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", filename, "config", "-q")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("validate generated Compose file: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
