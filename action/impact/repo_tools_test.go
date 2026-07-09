package impact

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matryer/is"
)

// newTestRepo creates a one-commit git repo with a known file.
func newTestRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=t@t",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=t@t",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	err := os.WriteFile(filepath.Join(dir, "orders.go"), []byte("package main\n// subscribes to ORDERS.>\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	run("init")
	run("add", ".")
	run("commit", "-m", "add orders consumer")
	return dir
}

func TestRepoToolsSearchReadListLogShow(t *testing.T) {
	is := is.New(t)
	repo, err := NewRepoTools(newTestRepo(t))
	is.NoErr(err)
	tools := repo.Tools()

	out, isErr := callTool(t, tools, "repo_search", `{"pattern":"ORDERS"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "orders.go:2"))

	out, isErr = callTool(t, tools, "repo_search", `{"pattern":"NO_SUCH_TOKEN"}`)
	is.True(!isErr)
	is.Equal(out, "(no matches)")

	out, isErr = callTool(t, tools, "repo_read", `{"path":"orders.go"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "package main"))

	out, isErr = callTool(t, tools, "repo_list", `{}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "orders.go"))

	out, isErr = callTool(t, tools, "repo_log", `{"path":"orders.go"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "add orders consumer"))

	out, isErr = callTool(t, tools, "repo_show", `{"ref":"HEAD"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "add orders consumer"))
}

func TestRepoToolsPathConfinement(t *testing.T) {
	is := is.New(t)
	repo, err := NewRepoTools(newTestRepo(t))
	is.NoErr(err)
	tools := repo.Tools()

	for _, path := range []string{"../secret", "/etc/passwd", "a/../../x"} {
		_, isErr := callTool(t, tools, "repo_read", `{"path":"`+path+`"}`)
		is.True(isErr) // path must be rejected: path
	}
}

func TestRepoToolsSymlinkEscape(t *testing.T) {
	is := is.New(t)
	// A secret outside the clone, reachable via a tracked symlink inside it.
	outside := filepath.Join(t.TempDir(), "secret")
	is.NoErr(os.WriteFile(outside, []byte("s3cret"), 0o600))
	dir := newTestRepo(t)
	is.NoErr(os.Symlink(outside, filepath.Join(dir, "link")))

	repo, err := NewRepoTools(dir)
	is.NoErr(err)
	out, isErr := callTool(t, repo.Tools(), "repo_read", `{"path":"link"}`)
	is.True(isErr) // symlink escaping the clone must not be readable
	is.True(!strings.Contains(out, "s3cret"))
}

func TestRepoToolsRefInjection(t *testing.T) {
	is := is.New(t)
	repo, err := NewRepoTools(newTestRepo(t))
	is.NoErr(err)
	tools := repo.Tools()

	_, isErr := callTool(t, tools, "repo_show", `{"ref":"--output=/tmp/pwned"}`)
	is.True(isErr)
	_, isErr = callTool(t, tools, "repo_show", `{"ref":""}`)
	is.True(isErr)
}

func TestNewRepoToolsRejectsMissingDir(t *testing.T) {
	is := is.New(t)
	_, err := NewRepoTools(filepath.Join(t.TempDir(), "nope"))
	is.True(err != nil)
}
