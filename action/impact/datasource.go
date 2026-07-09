package impact

import (
	"context"
	"encoding/json"
)

// Tool pairs a provider-neutral tool definition with its handler. Handlers
// return the tool result and whether it is an error the model should see
// (is_error semantics — the run continues either way).
type Tool struct {
	Def     ToolDef
	Handler func(ctx context.Context, input json.RawMessage) (result string, isError bool)
}

// DataSource is one view of operational reality. Each configured source
// contributes its tools (namespaced by source name) and a Describe() section
// injected into the agent's instructions so the agent knows what the source
// can answer and how to query it.
type DataSource interface {
	// Name identifies the source in config, reports, and evidence citations.
	Name() string
	// Describe returns documentation injected into the agent's context.
	Describe(ctx context.Context) (string, error)
	// Tools returns the tools this source exposes to the agent.
	Tools() []Tool
	// HealthCheck verifies connectivity before the agent starts, so a
	// misconfigured source fails the run instead of silently degrading it.
	HealthCheck(ctx context.Context) error
}

// truncateResult caps a tool result so one oversized response cannot blow the
// context window or the token budget.
const maxToolResultBytes = 100_000

func truncateResult(s string) string {
	if len(s) <= maxToolResultBytes {
		return s
	}
	return s[:maxToolResultBytes] + "\n... (truncated)"
}
