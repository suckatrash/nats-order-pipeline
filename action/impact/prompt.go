package impact

import (
	_ "embed"
	"fmt"
	"strings"
)

// skillMarkdown is the impact-analysis skill: the analysis methodology, the
// finding catalog, the SQL playbooks, and the pitfalls. It is the canonical
// copy; .claude/skills/impact-analysis/SKILL.md symlinks here so interactive
// Claude Code sessions and this binary share one source of truth.
//
//go:embed SKILL.md
var skillMarkdown string

// buildSystemPrompt assembles the agent's instructions: the skill, one
// section per data source, the repo toolset (when a clone is available), and
// the binary-specific operating rules.
func buildSystemPrompt(sourceDocs []string, repoDoc string, cfg *Config) string {
	var b strings.Builder
	b.WriteString(skillMarkdown)
	b.WriteString("\n\n# Execution environment\n\n")
	b.WriteString("You are running inside the one-shot `impact` CLI. The skill above describes the methodology; the sections below describe the tools available in this run.\n")

	for _, doc := range sourceDocs {
		b.WriteString("\n" + doc + "\n")
	}
	if repoDoc != "" {
		b.WriteString("\n" + repoDoc + "\n")
	}

	b.WriteString(`
## Reporting

The report is built through tools, not prose:

- emit_finding: record a catalog finding with its evidence, as soon as it is substantiated.
- emit_note: record a non-scored, evidence-backed observation.
- set_summary: set the change summary, data epoch, affected entities, and recommendations. Call it once, near the end, after all findings are in.

The risk level is derived mechanically from the finding codes — you do not set it. Your final text message is not the report (it is discarded except as a fallback change summary); everything that matters must go through the tools.

Evidence citations are verified against this run's execution log: each evidence "query" must be the verbatim SQL or tool input you actually ran (or a file path/line from the diff or a repo tool call). A citation that does not match an executed query is rejected — re-cite exactly, or run the query first.

## Operating rules

`)
	fmt.Fprintf(&b, "- Freshness gate: suppress findings whose evidence is older than %s; note degraded data instead.\n", cfg.Findings.StalenessBound.Std())
	fmt.Fprintf(&b, "- HEADROOM_EXHAUSTION lookahead horizon: %s.\n", cfg.Findings.HeadroomHorizon.Std())
	b.WriteString(`- There is no user to ask: never request confirmation or clarification. Resolve ambiguity from data, or surface it in the report.
- Keep tool results lean: prefer targeted queries with LIMIT over broad scans.
- You operate under a token budget; be economical. When told the budget is nearly exhausted, stop investigating and finalize the report with what is already substantiated.`)
	return b.String()
}

// initialUserMessage frames the diff for the first turn.
func initialUserMessage(diff string) string {
	return "Analyze the impact of this change:\n\n```diff\n" + diff + "\n```"
}
