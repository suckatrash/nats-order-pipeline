package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// evidenceLog records what the agent actually executed this run — the string
// values of every successful tool input, keyed by the source the tool belongs
// to — so evidence citations are checked against performed work instead of
// taken on faith. A finding citing a query that was never run is rejected at
// the tool boundary.
//
// Not safe for concurrent use: it assumes the analyzer's sequential tool
// execution.
type evidenceLog struct {
	recorded map[string][]string
}

func newEvidenceLog() *evidenceLog {
	return &evidenceLog{recorded: map[string][]string{}}
}

// normalizeCitation makes matching robust to reformatting: whitespace runs
// collapse to a single space, trailing semicolons drop, and case is folded
// (SQL keywords and identifiers are routinely re-cased between execution and
// citation).
func normalizeCitation(s string) string {
	s = strings.ToLower(strings.Join(strings.Fields(s), " "))
	return strings.TrimRight(s, "; ")
}

// citableKeys are the tool-input fields whose values constitute citable work:
// executed SQL and the repo tools' path/pattern/ref arguments. Discovery-tool
// inputs (schema and table names) are deliberately excluded — a table name
// seen during discovery must not license a fabricated query that mentions it.
var citableKeys = map[string]bool{"sql": true, "path": true, "pattern": true, "ref": true}

// record captures the citable fields of a successful tool input under the
// owning source.
func (l *evidenceLog) record(source string, input json.RawMessage) {
	var parsed any
	if err := json.Unmarshal(input, &parsed); err != nil {
		return
	}
	key := strings.ToLower(source)
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, mv := range t {
				if s, ok := mv.(string); ok && citableKeys[k] {
					if n := normalizeCitation(s); n != "" {
						l.recorded[key] = append(l.recorded[key], n)
					}
					continue
				}
				walk(mv)
			}
		case []any:
			for _, av := range t {
				walk(av)
			}
		}
	}
	walk(parsed)
}

// recordText seeds the log with material the agent received without a tool
// call — the diff itself, recorded under "repo", so diff-line citations
// verify even when no repo tool touched the file.
func (l *evidenceLog) recordText(source, text string) {
	if s := normalizeCitation(text); s != "" {
		l.recorded[strings.ToLower(source)] = append(l.recorded[strings.ToLower(source)], s)
	}
}

// repoLineSuffix strips line references (":8", ":8-12", ":8,12", "#L8") from
// repo citations before matching: the executed input names the file, the
// citation a line.
var repoLineSuffix = regexp.MustCompile(`(:|#L)\d+([-,]\d+)*$`)

// containMin is the minimum normalized length before substring containment
// counts as a match; short fragments must match exactly.
const containMin = 8

// verify checks one evidence entry against the log. The zero return is nil;
// a non-nil error carries the reason phrased for the model to correct.
func (l *evidenceLog) verify(e Evidence) error {
	key := strings.ToLower(e.Source)
	recs, ok := l.recorded[key]
	if !ok {
		known := slices.Sorted(maps.Keys(l.recorded))
		return fmt.Errorf("evidence source %q is not one this run queried — use one of: %s", e.Source, strings.Join(known, ", "))
	}
	citation := e.Query
	if key == "repo" {
		citation = repoLineSuffix.ReplaceAllString(citation, "")
	}
	citation = normalizeCitation(citation)
	for _, rec := range recs {
		if rec == citation {
			return nil
		}
		// The citation may be a fragment of recorded work (a path inside the
		// seeded diff, part of a long executed query) — but never a superset:
		// extending an executed query with invented clauses must not verify.
		if len(citation) >= containMin && strings.Contains(rec, citation) {
			return nil
		}
	}
	return fmt.Errorf("evidence query %q was not executed against source %q this run — cite the verbatim query or tool input you ran", e.Query, e.Source)
}

// recordingHandler logs the tool input to the evidence log when the call
// succeeds — a failed query is not citable evidence, and neither is a search
// that matched nothing.
func recordingHandler(evlog *evidenceLog, source string, h func(context.Context, json.RawMessage) (string, bool)) func(context.Context, json.RawMessage) (string, bool) {
	return func(ctx context.Context, input json.RawMessage) (string, bool) {
		content, isErr := h(ctx, input)
		if !isErr && content != "(no matches)" {
			evlog.record(source, input)
		}
		return content, isErr
	}
}
