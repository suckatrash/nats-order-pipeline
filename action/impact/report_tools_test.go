package impact

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matryer/is"
)

func callTool(t *testing.T, tools []Tool, name, input string) (string, bool) {
	t.Helper()
	for _, tool := range tools {
		if tool.Def.Name == name {
			// Every tool schema must itself be valid JSON.
			var schema map[string]any
			if err := json.Unmarshal(tool.Def.InputSchema, &schema); err != nil {
				t.Fatalf("tool %s has invalid schema: %v", name, err)
			}
			return tool.Handler(context.Background(), json.RawMessage(input))
		}
	}
	t.Fatalf("tool %s not found", name)
	return "", false
}

func TestEmitFindingValidation(t *testing.T) {
	is := is.New(t)
	rb := &reportBuilder{}
	tools := rb.Tools()

	// Unknown code is rejected: the catalog is fixed.
	_, isErr := callTool(t, tools, "emit_finding", `{"code":"VIBES","summary":"x","evidence":[{"source":"insights","query":"q","value":"v"}]}`)
	is.True(isErr)

	// Missing evidence is rejected: no measurement, no finding.
	_, isErr = callTool(t, tools, "emit_finding", `{"code":"DATA_LOSS","summary":"x","evidence":[]}`)
	is.True(isErr)

	// Incomplete evidence is rejected.
	_, isErr = callTool(t, tools, "emit_finding", `{"code":"DATA_LOSS","summary":"x","evidence":[{"source":"insights"}]}`)
	is.True(isErr)

	// A substantiated finding is recorded.
	_, isErr = callTool(t, tools, "emit_finding", `{"code":"DATA_LOSS","summary":"lagging consumer loses 41k msgs","evidence":[{"source":"insights","query":"SELECT lag ...","value":"41203","epoch":"2026-07-09T14:32:00Z"}]}`)
	is.True(!isErr)
	is.Equal(len(rb.report.Findings), 1)
	is.Equal(rb.report.Findings[0].Code, CodeDataLoss)
}

func TestEmitNoteRequiresEvidence(t *testing.T) {
	is := is.New(t)
	rb := &reportBuilder{}
	tools := rb.Tools()

	_, isErr := callTool(t, tools, "emit_note", `{"text":"might be risky","evidence":[]}`)
	is.True(isErr)

	_, isErr = callTool(t, tools, "emit_note", `{"text":"no retry path","evidence":[{"source":"repo","query":"a/b.go:12","value":"js.Subscribe(...)"}]}`)
	is.True(!isErr)
	is.Equal(len(rb.report.Notes), 1)
}

func TestSetSummary(t *testing.T) {
	is := is.New(t)
	rb := &reportBuilder{}
	tools := rb.Tools()

	_, isErr := callTool(t, tools, "set_summary", `{}`)
	is.True(isErr) // change_summary required

	_, isErr = callTool(t, tools, "set_summary", `{
		"change_summary":"replicas 3->1 on ORDERS",
		"data_epoch":"2026-07-09T14:32:00Z",
		"affected_entities":[{"entity":"order-proc","type":"consumer","relationship":"consumes ORDERS"}],
		"recommendations":["do it off-peak"]
	}`)
	is.True(!isErr)
	is.Equal(rb.report.ChangeSummary, "replicas 3->1 on ORDERS")
	is.Equal(len(rb.report.AffectedEntities), 1)
	is.Equal(len(rb.report.Recommendations), 1)
}
