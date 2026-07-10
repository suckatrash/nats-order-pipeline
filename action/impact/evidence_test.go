package impact

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestEvidenceLogVerify(t *testing.T) {
	is := is.New(t)
	l := newEvidenceLog()
	l.record("insights", json.RawMessage(`{"sql":"SELECT msgs, bytes FROM hx.stream_replica_stats WHERE stream_pk=3"}`))

	// Verbatim citation passes.
	is.NoErr(l.verify(Evidence{Source: "insights", Query: "SELECT msgs, bytes FROM hx.stream_replica_stats WHERE stream_pk=3"}))
	// Re-cased, re-wrapped, and semicolon-terminated citations still pass.
	is.NoErr(l.verify(Evidence{Source: "Insights", Query: "select msgs,\n  bytes from HX.STREAM_REPLICA_STATS\n  where stream_pk=3;"}))
	// A fragment of the executed query passes.
	is.NoErr(l.verify(Evidence{Source: "insights", Query: "FROM hx.stream_replica_stats"}))
	// A query that was never run is rejected.
	err := l.verify(Evidence{Source: "insights", Query: "SELECT lag FROM hx.consumer_replica_stats"})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "was not executed"))
	// A superset of the executed query is rejected: extending real work with
	// invented clauses must not verify.
	is.True(l.verify(Evidence{Source: "insights", Query: "SELECT msgs, bytes FROM hx.stream_replica_stats WHERE stream_pk=3 AND deleted=1"}) != nil)
	// A table name seen only in a discovery call does not license a
	// fabricated query mentioning it: discovery inputs are not citable.
	l.record("insights", json.RawMessage(`{"schema":"hx","table":"consumer_ident"}`))
	is.True(l.verify(Evidence{Source: "insights", Query: "SELECT * FROM hx.consumer_ident"}) != nil)
	// An unknown source is rejected with the known ones listed.
	err = l.verify(Evidence{Source: "prometheus", Query: "up"})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "insights"))
}

func TestEvidenceLogRepoCitations(t *testing.T) {
	is := is.New(t)
	l := newEvidenceLog()
	// The diff is seeded as repo material; file:line citations must verify
	// against it with the line suffix stripped.
	l.recordText("repo", "diff --git a/deploy/streams/orders.json b/deploy/streams/orders.json\n-  \"max_bytes\": 5368709120,\n+  \"max_bytes\": 268435456,")
	is.NoErr(l.verify(Evidence{Source: "repo", Query: "deploy/streams/orders.json:8"}))
	is.NoErr(l.verify(Evidence{Source: "repo", Query: "deploy/streams/orders.json:8-12"}))
	is.NoErr(l.verify(Evidence{Source: "repo", Query: "deploy/streams/orders.json#L8"}))
	err := l.verify(Evidence{Source: "repo", Query: "deploy/streams/other.json:3"})
	is.True(err != nil)

	// Repo tool inputs record their citable fields too.
	l.record("repo", json.RawMessage(`{"path":"cmd/enricher/main.go"}`))
	is.NoErr(l.verify(Evidence{Source: "repo", Query: "cmd/enricher/main.go:41"}))
}

func TestEvidenceLogContainmentGuard(t *testing.T) {
	is := is.New(t)
	l := newEvidenceLog()
	l.record("insights", json.RawMessage(`{"sql":"x"}`))
	// Short fragments only match exactly — "x" must not substring-match
	// its way into verifying an arbitrary citation.
	is.NoErr(l.verify(Evidence{Source: "insights", Query: "x"}))
	is.True(l.verify(Evidence{Source: "insights", Query: "SELECT x FROM y"}) != nil)
}

func TestRecordingHandlerSkipsFailedCalls(t *testing.T) {
	is := is.New(t)
	l := newEvidenceLog()
	// A failed call is not citable evidence and must not be recorded; a
	// successful one is.
	fail := recordingHandler(l, "insights", func(_ context.Context, _ json.RawMessage) (string, bool) {
		return "syntax error", true
	})
	_, isErr := fail(context.Background(), json.RawMessage(`{"sql":"SELECT broken"}`))
	is.True(isErr)
	is.True(l.verify(Evidence{Source: "insights", Query: "SELECT broken"}) != nil)

	// A search that matched nothing is not citable work either.
	empty := recordingHandler(l, "repo", func(_ context.Context, _ json.RawMessage) (string, bool) {
		return "(no matches)", false
	})
	_, isErr = empty(context.Background(), json.RawMessage(`{"pattern":"missing_setting"}`))
	is.True(!isErr)
	is.True(l.verify(Evidence{Source: "repo", Query: "missing_setting"}) != nil)

	ok := recordingHandler(l, "insights", func(_ context.Context, _ json.RawMessage) (string, bool) {
		return "[]", false
	})
	_, isErr = ok(context.Background(), json.RawMessage(`{"sql":"SELECT broken"}`))
	is.True(!isErr)
	is.NoErr(l.verify(Evidence{Source: "insights", Query: "SELECT broken"}))
}
