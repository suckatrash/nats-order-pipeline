package impact

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func natsdocsSourceForTest(t *testing.T) DataSource {
	t.Helper()
	src, err := NewNatsdocs()
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// The constructor validates the vendored corpus: every operation in the
// dataset must have its rendered document and a known group, so a bad
// refresh fails here.
func TestNatsdocsCorpusIsConsistent(t *testing.T) {
	is := is.New(t)
	src := natsdocsSourceForTest(t)
	is.Equal(src.Name(), "natsdocs")
	is.NoErr(src.HealthCheck(context.Background()))

	// Every loadable operation appears in the Describe index — nothing the
	// agent could look up is hidden from it.
	desc, err := src.Describe(context.Background())
	is.NoErr(err)
	for id := range src.(*natsdocsSource).docs {
		is.True(strings.Contains(desc, "- "+id+" (")) // id listed in the index
	}
}

// entryRe asserts index-entry format without pinning the vendored corpus's
// tier classifications, which a refresh may legitimately re-score.
var entryRe = regexp.MustCompile(`jetstream\.PurgeStream \((minimal|low|moderate|high)\): `)

func TestNatsdocsDescribeIndexesOperations(t *testing.T) {
	is := is.New(t)
	src := natsdocsSourceForTest(t)
	desc, err := src.Describe(context.Background())
	is.NoErr(err)
	is.True(strings.Contains(desc, "Core NATS:"))
	is.True(entryRe.MatchString(desc))
	// The citation rule separating reference knowledge from live observation.
	is.True(strings.Contains(desc, "inherent cost"))
}

func TestNatsdocsLookupReturnsDocument(t *testing.T) {
	is := is.New(t)
	src := natsdocsSourceForTest(t)

	out, isErr := callTool(t, src.Tools(), "natsdocs_lookup", `{"ref":"jetstream.PurgeStream"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "# jetstream.PurgeStream"))
	is.True(strings.Contains(out, "Tier: "))
	is.True(strings.Contains(out, "In practice"))

	out, isErr = callTool(t, src.Tools(), "natsdocs_lookup", `{"ref":"jetstream.NoSuchOp"}`)
	is.True(isErr)
	is.True(strings.Contains(out, "unknown operation"))

	_, isErr = callTool(t, src.Tools(), "natsdocs_lookup", `{}`)
	is.True(isErr) // ref is required
}

func TestNatsdocsSearchFindsOperationsByContent(t *testing.T) {
	is := is.New(t)
	src := natsdocsSourceForTest(t)

	out, isErr := callTool(t, src.Tools(), "natsdocs_search", `{"match":"$JS.API.STREAM.PURGE"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "jetstream.PurgeStream:"))

	out, isErr = callTool(t, src.Tools(), "natsdocs_search", `{"match":"zzz-no-such-content"}`)
	is.True(!isErr)
	is.Equal(out, "(no matches)")

	// A broad match is capped, not dumped.
	out, isErr = callTool(t, src.Tools(), "natsdocs_search", `{"match":"e"}`)
	is.True(!isErr)
	is.True(strings.Contains(out, "more matches elided"))
	is.True(len(strings.Split(strings.TrimSpace(out), "\n")) <= maxSearchLines+1)

	_, isErr = callTool(t, src.Tools(), "natsdocs_search", `{}`)
	is.True(isErr) // match is required
}

// The lookup ref is recorded under the citable "ref" key, so evidence citing
// the operation id verifies; search inputs are discovery and do not.
func TestNatsdocsLookupIsCitableEvidence(t *testing.T) {
	is := is.New(t)
	src := natsdocsSourceForTest(t)

	evlog := newEvidenceLog()
	tools := src.Tools()
	for i := range tools {
		tools[i].Handler = recordingHandler(evlog, src.Name(), tools[i].Def, tools[i].Handler)
	}
	_, isErr := callTool(t, tools, "natsdocs_lookup", `{"ref":"jetstream.DoubleAck"}`)
	is.True(!isErr)
	_, isErr = callTool(t, tools, "natsdocs_search", `{"match":"jetstream.Consume"}`)
	is.True(!isErr)

	is.NoErr(evlog.verify(Evidence{Source: "natsdocs", Query: "jetstream.DoubleAck"}))
	// A search input is not citable work.
	is.True(evlog.verify(Evidence{Source: "natsdocs", Query: "jetstream.Consume"}) != nil)
	// A never-looked-up operation does not verify.
	is.True(evlog.verify(Evidence{Source: "natsdocs", Query: "jetstream.PurgeStream"}) != nil)

	// A citable key nested inside an ignored field is padding, not work —
	// only the top-level ref the handler actually reads is recorded.
	_, isErr = callTool(t, tools, "natsdocs_lookup",
		`{"ref":"jetstream.Ack","x":{"ref":"jetstream.PurgeStream"}}`)
	is.True(!isErr)
	is.NoErr(evlog.verify(Evidence{Source: "natsdocs", Query: "jetstream.Ack"}))
	is.True(evlog.verify(Evidence{Source: "natsdocs", Query: "jetstream.PurgeStream"}) != nil)
}

func TestNatsdocsDisabledByConfig(t *testing.T) {
	is := is.New(t)
	off := false
	is.True((*NatsdocsConfig)(nil).IsEnabled())
	is.True((&NatsdocsConfig{}).IsEnabled())
	is.True(!(&NatsdocsConfig{Enabled: &off}).IsEnabled())
}
