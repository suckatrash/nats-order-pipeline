package impact

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// The knowledge directory vendors the nats-op-costs corpus — a deterministic
// cost classification of every nats.go client operation, cross-checked
// against nats-server code paths. operations.json is the canonical dataset;
// ops/<id>.md are the rendered per-operation documents the lookup tool
// serves. Refresh both with action/refresh-knowledge.sh; do not edit by hand.
//
//go:embed knowledge/operations.json knowledge/ops/*.md
var knowledgeFS embed.FS

// natsdocsSource exposes the embedded corpus as agent tools. Unlike insights
// and prometheus it is reference knowledge, not live observation: it grounds
// claims about what an operation inherently costs, never about the current
// state of the system under analysis.
type natsdocsSource struct {
	describe string
	docs     map[string]string // operation id -> rendered markdown document
}

// knowledgeDataset is the subset of the nats-op-costs dataset the source
// needs. Parsed loosely (unknown fields ignored) so corpus refreshes that add
// fields do not break the build.
type knowledgeDataset struct {
	Sources map[string]struct {
		Commit   string `json:"commit"`
		Describe string `json:"describe"`
	} `json:"sources"`
	Operations []knowledgeOp `json:"operations"`
}

type knowledgeOp struct {
	ID       string `json:"id"`
	Group    string `json:"group"`
	Summary  string `json:"summary"`
	CostTier string `json:"cost_tier"`
}

// knowledgeGroups fixes the display order of operation groups in the source
// description (mirrors the corpus's own rendering order).
type knowledgeGroup struct{ key, title string }

var knowledgeGroups = []knowledgeGroup{
	{"core", "Core NATS"},
	{"jetstream", "JetStream"},
	{"kv", "Key-Value"},
	{"objectstore", "Object Store"},
}

// NewNatsdocs builds the source from the embedded corpus. Errors here mean
// the vendored files are inconsistent (e.g. an operation without its
// document after a bad refresh) and are caught by the package tests.
func NewNatsdocs() (DataSource, error) {
	raw, err := knowledgeFS.ReadFile("knowledge/operations.json")
	if err != nil {
		return nil, fmt.Errorf("embedded knowledge dataset: %w", err)
	}
	var ds knowledgeDataset
	if err := json.Unmarshal(raw, &ds); err != nil {
		return nil, fmt.Errorf("parse knowledge dataset: %w", err)
	}
	if len(ds.Operations) == 0 {
		return nil, fmt.Errorf("knowledge dataset lists no operations")
	}
	docs := make(map[string]string, len(ds.Operations))
	for _, op := range ds.Operations {
		doc, err := knowledgeFS.ReadFile("knowledge/ops/" + op.ID + ".md")
		if err != nil {
			return nil, fmt.Errorf("knowledge document for %s: %w", op.ID, err)
		}
		docs[op.ID] = string(doc)
		// An operation outside the known groups would silently vanish from
		// the Describe index while staying loadable — fail the refresh loudly
		// instead.
		if !slices.ContainsFunc(knowledgeGroups, func(g knowledgeGroup) bool { return g.key == op.Group }) {
			return nil, fmt.Errorf("operation %s has unknown group %q — update knowledgeGroups", op.ID, op.Group)
		}
	}
	return &natsdocsSource{describe: describeKnowledge(&ds), docs: docs}, nil
}

// describeKnowledge renders the source documentation injected into the
// agent's context: usage and citation rules, then the full operation index —
// id, tier, summary per group — so the agent knows what is loadable without
// spending a tool call.
func describeKnowledge(ds *knowledgeDataset) string {
	var b strings.Builder
	b.WriteString(`## Data source: natsdocs

The natsdocs_* tools consult an embedded reference corpus: a deterministic
cost classification of every nats.go client operation, cross-checked against
nats-server code paths`)
	var pins []string
	for _, repo := range []string{"nats.go", "nats-server"} {
		if s, ok := ds.Sources[repo]; ok {
			pins = append(pins, fmt.Sprintf("%s %s", repo, s.Describe))
		}
	}
	if len(pins) > 0 {
		fmt.Fprintf(&b, " (pinned: %s)", strings.Join(pins, ", "))
	}
	b.WriteString(`. Consult it when a change adds, removes, moves, or
reconfigures NATS client operations, to ground what those operations
inherently cost and what they contend with.

- natsdocs_lookup(ref) returns the document for one operation: cost tier and
  score, invocation facts (round trips, disk I/O, Raft, server state, scans,
  choke points), the request flow with nats-server code references, practical
  guidance, and the contention groups it serializes with.
- natsdocs_search(match) greps the corpus when the operation is not obvious
  from the index below (e.g. by API subject, lock name, or behavior).
- Cite evidence from this source as source "natsdocs" with the operation id
  you looked up. A natsdocs citation supports claims about an operation's
  inherent cost and contention behavior ONLY — any claim about the current
  state of the live system still requires insights or prometheus evidence.
- Tier rubric: minimal < low < moderate < high, a pure function of the
  operation's facts; steady-state work is double-weighted because it recurs
  for the lifetime of the operation.

Operation index (id (tier): summary):`)
	for _, g := range knowledgeGroups {
		ops := slices.Clone(ds.Operations)
		ops = slices.DeleteFunc(ops, func(op knowledgeOp) bool { return op.Group != g.key })
		slices.SortFunc(ops, func(a, b knowledgeOp) int { return strings.Compare(a.ID, b.ID) })
		fmt.Fprintf(&b, "\n\n%s:\n", g.title)
		for _, op := range ops {
			fmt.Fprintf(&b, "- %s (%s): %s\n", op.ID, op.CostTier, op.Summary)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func (s *natsdocsSource) Name() string { return "natsdocs" }

func (s *natsdocsSource) Describe(ctx context.Context) (string, error) {
	return s.describe, nil
}

// HealthCheck is a no-op: the corpus is embedded and validated at
// construction, so there is nothing left to fail at run time.
func (s *natsdocsSource) HealthCheck(ctx context.Context) error { return nil }

func (s *natsdocsSource) Tools() []Tool {
	return []Tool{
		{
			Def: ToolDef{
				Name:        "natsdocs_lookup",
				Description: "Return the reference document for one NATS client operation: cost tier, invocation facts, request flow, practical guidance, and contention groups. The ref is an operation id from the natsdocs index (e.g. jetstream.PurgeStream).",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"ref":{"type":"string","description":"Operation id, e.g. jetstream.Publish, kv.Watch, nats.Subscribe"}},"required":["ref"]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Ref string `json:"ref"`
				}
				if err := json.Unmarshal(input, &in); err != nil || in.Ref == "" {
					return "invalid input: an operation ref is required (e.g. jetstream.Publish)", true
				}
				doc, ok := s.docs[in.Ref]
				if !ok {
					return fmt.Sprintf("unknown operation %q — use an id from the natsdocs index, or natsdocs_search to find it", in.Ref), true
				}
				return truncateResult(doc), false
			},
		},
		{
			Def: ToolDef{
				Name:        "natsdocs_search",
				Description: "Case-insensitive substring search across all operation documents. Returns matching lines prefixed with the operation id. Use to find operations by API subject, lock name, or behavior; then natsdocs_lookup the id.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"match":{"type":"string","description":"Substring to search for (e.g. $JS.API.STREAM.PURGE, write lock, redelivery)"}},"required":["match"]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Match string `json:"match"`
				}
				if err := json.Unmarshal(input, &in); err != nil || in.Match == "" {
					return "invalid input: a match string is required", true
				}
				return s.search(in.Match)
			},
		},
	}
}

// maxSearchLines caps search output; past this the match string is too broad
// to be useful and the model should narrow it.
const maxSearchLines = 60

func (s *natsdocsSource) search(match string) (string, bool) {
	needle := strings.ToLower(match)
	var b strings.Builder
	lines := 0
	for _, id := range slices.Sorted(maps.Keys(s.docs)) {
		for _, line := range strings.Split(s.docs[id], "\n") {
			if !strings.Contains(strings.ToLower(line), needle) {
				continue
			}
			if lines == maxSearchLines {
				b.WriteString("... (more matches elided — narrow the match)\n")
				return b.String(), false
			}
			fmt.Fprintf(&b, "%s: %s\n", id, strings.TrimSpace(line))
			lines++
		}
	}
	if lines == 0 {
		return "(no matches)", false
	}
	return b.String(), false
}
