package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// RepoTools exposes read-only inspection of the caller-provided clone. All
// git operations run via the git binary with the repo as -C target; paths
// are confined to the clone and refs are guarded against option injection.
type RepoTools struct {
	dir string
}

// NewRepoTools validates dir and returns the repo toolset. The directory
// must exist; it need not be a git repo (search/read degrade to git errors
// the model can see).
func NewRepoTools(dir string) (*RepoTools, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("repo dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("repo path %s is not a directory", dir)
	}
	return &RepoTools{dir: abs}, nil
}

// Describe documents the toolset for the agent prompt.
func (r *RepoTools) Describe() string {
	return `## Repository access

A local clone of the repository the diff applies to is available:

- repo_search(pattern, path?): git grep for a pattern; returns file:line matches.
- repo_read(path): read one file (truncated past 50 KiB).
- repo_list(path?): list tracked files, optionally under a path prefix.
- repo_log(path?, max?): recent commit history (oneline), optionally for one path.
- repo_show(ref): a commit's message and patch.

History depth depends on the caller's clone; a shallow clone may have little
of it. Cite repo evidence as source "repo" with file:line in the query field.`
}

func (r *RepoTools) Tools() []Tool {
	return []Tool{
		{
			Def: ToolDef{
				Name:        "repo_search",
				Description: "Search the repository for a pattern (git grep -n). Returns file:line matches.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"pattern":{"type":"string","description":"Regex pattern"},"path":{"type":"string","description":"Optional path prefix to restrict the search"}},"required":["pattern"]}`),
			},
			Handler: r.search,
		},
		{
			Def: ToolDef{
				Name:        "repo_read",
				Description: "Read a file from the repository. Path is relative to the repo root.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
			},
			Handler: r.read,
		},
		{
			Def: ToolDef{
				Name:        "repo_list",
				Description: "List tracked files in the repository, optionally under a path prefix.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
			},
			Handler: r.list,
		},
		{
			Def: ToolDef{
				Name:        "repo_log",
				Description: "Show recent commit history (oneline), optionally restricted to a path.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"max":{"type":"integer","description":"Max commits (default 30)"}}}`),
			},
			Handler: r.log,
		},
		{
			Def: ToolDef{
				Name:        "repo_show",
				Description: "Show a commit: message, stats, and patch.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"ref":{"type":"string","description":"Commit SHA or ref"}},"required":["ref"]}`),
			},
			Handler: r.show,
		},
	}
}

// sanitizePath confines a model-supplied path to the clone. filepath.IsLocal
// rejects absolute paths, ".." escapes, and (on Windows) reserved names.
func (r *RepoTools) sanitizePath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("path is required")
	}
	clean := filepath.Clean(p)
	if !filepath.IsLocal(clean) {
		return "", fmt.Errorf("path must be relative and inside the repository")
	}
	return clean, nil
}

// sanitizeRef rejects ref values that could be parsed as git options.
func sanitizeRef(ref string) (string, error) {
	if ref == "" || strings.HasPrefix(ref, "-") {
		return "", fmt.Errorf("invalid ref")
	}
	return ref, nil
}

// git runs one git command in the clone and returns its combined output.
func (r *RepoTools) git(ctx context.Context, args ...string) (string, bool) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", r.dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// git grep exits 1 on "no matches" with empty output — not an error
		// worth surfacing as one.
		if len(out) == 0 {
			if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
				return "(no matches)", false
			}
		}
		return fmt.Sprintf("git error: %v\n%s", err, truncateResult(string(out))), true
	}
	if len(out) == 0 {
		return "(empty)", false
	}
	return truncateResult(string(out)), false
}

func (r *RepoTools) search(ctx context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	if params.Pattern == "" {
		return "pattern is required", true
	}
	args := []string{"grep", "-n", "-I", "-e", params.Pattern}
	if params.Path != "" {
		p, err := r.sanitizePath(params.Path)
		if err != nil {
			return err.Error(), true
		}
		args = append(args, "--", p)
	}
	return r.git(ctx, args...)
}

func (r *RepoTools) read(_ context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	p, err := r.sanitizePath(params.Path)
	if err != nil {
		return err.Error(), true
	}
	// os.Root confines the read to the clone even through symlinks — a
	// tracked symlink pointing outside the repo fails instead of escaping
	// the sandbox sanitizePath establishes for the lexical path.
	root, err := os.OpenRoot(r.dir)
	if err != nil {
		return fmt.Sprintf("read error: %v", err), true
	}
	defer root.Close()
	data, err := root.ReadFile(p)
	if err != nil {
		return fmt.Sprintf("read error: %v", err), true
	}
	const maxBytes = 50_000
	if len(data) > maxBytes {
		return string(data[:maxBytes]) + "\n... (truncated)", false
	}
	return string(data), false
}

func (r *RepoTools) list(ctx context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	args := []string{"ls-files"}
	if params.Path != "" {
		p, err := r.sanitizePath(params.Path)
		if err != nil {
			return err.Error(), true
		}
		args = append(args, "--", p)
	}
	return r.git(ctx, args...)
}

func (r *RepoTools) log(ctx context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		Path string `json:"path"`
		Max  int    `json:"max"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	if params.Max <= 0 || params.Max > 200 {
		params.Max = 30
	}
	args := []string{"log", "--oneline", "--no-decorate", "-n", strconv.Itoa(params.Max)}
	if params.Path != "" {
		p, err := r.sanitizePath(params.Path)
		if err != nil {
			return err.Error(), true
		}
		args = append(args, "--", p)
	}
	return r.git(ctx, args...)
}

func (r *RepoTools) show(ctx context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		Ref string `json:"ref"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	ref, err := sanitizeRef(params.Ref)
	if err != nil {
		return err.Error(), true
	}
	// The trailing "--" forces rev interpretation, removing rev/path
	// ambiguity for odd but valid ref spellings.
	return r.git(ctx, "show", "--stat", "--patch", ref, "--")
}
