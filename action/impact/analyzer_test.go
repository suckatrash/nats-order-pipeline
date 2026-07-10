package impact

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

// fakeProvider replays scripted turns and records what was sent to it.
type fakeProvider struct {
	turns  []*Turn
	system string
	defs   []ToolDef
	sent   []sentMsg
}

type sentMsg struct {
	text    string
	results []ToolResult
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Start(system string, tools []ToolDef) Conversation {
	p.system = system
	p.defs = tools
	return &fakeConversation{p: p}
}

type fakeConversation struct {
	p *fakeProvider
	i int
}

func (c *fakeConversation) Send(_ context.Context, text string, results []ToolResult) (*Turn, error) {
	// Copy results: the analyzer reuses its slice across turns.
	cp := append([]ToolResult(nil), results...)
	c.p.sent = append(c.p.sent, sentMsg{text: text, results: cp})
	if c.i >= len(c.p.turns) {
		return nil, errors.New("fake provider: out of scripted turns")
	}
	t := c.p.turns[c.i]
	c.i++
	return t, nil
}

// fakeSource is a minimal healthy data source with one recording tool.
type fakeSource struct {
	queries []string
}

func (s *fakeSource) Name() string { return "test" }
func (s *fakeSource) Describe(context.Context) (string, error) {
	return "## Data source: test\ndocs here", nil
}
func (s *fakeSource) HealthCheck(context.Context) error { return nil }
func (s *fakeSource) Tools() []Tool {
	return []Tool{{
		Def: ToolDef{Name: "test_query", Description: "q", InputSchema: json.RawMessage(`{"type":"object","properties":{"sql":{"type":"string"}}}`)},
		Handler: func(_ context.Context, input json.RawMessage) (string, bool) {
			s.queries = append(s.queries, string(input))
			return `[{"lag":41203}]`, false
		},
	}}
}

func testConfig() *Config {
	cfg := DefaultConfig()
	cfg.Agent.APIKey = "k"
	cfg.Agent.Timeout = Duration(time.Minute)
	cfg.Datasources.Insights = &InsightsConfig{Endpoint: "nats://x"}
	return cfg
}

func TestAnalyzerHappyPath(t *testing.T) {
	is := is.New(t)
	src := &fakeSource{}
	provider := &fakeProvider{turns: []*Turn{
		{
			ToolCalls: []ToolCall{
				{ID: "1", Name: "test_query", Input: json.RawMessage(`{"sql":"SELECT lag"}`)},
				{ID: "2", Name: "emit_finding", Input: json.RawMessage(`{"code":"DATA_LOSS","summary":"41k unprocessed messages lost","evidence":[{"source":"test","query":"SELECT lag","value":"41203","epoch":"e1"}]}`)},
				{ID: "3", Name: "set_summary", Input: json.RawMessage(`{"change_summary":"delete stream ORDERS","data_epoch":"e1"}`)},
			},
			Usage: TokenUsage{InputTokens: 1000, OutputTokens: 100},
		},
		{Text: "done", StopReason: "end_turn", Usage: TokenUsage{InputTokens: 1200, OutputTokens: 50}},
	}}

	a := NewAnalyzer(testConfig(), provider, []DataSource{src}, nil, nil)
	report, err := a.Run(context.Background(), "diff --git a/x b/x")
	is.NoErr(err)

	is.Equal(report.ChangeSummary, "delete stream ORDERS")
	is.Equal(report.RiskLevel, RiskCritical)
	is.Equal(report.DataEpoch, "e1")
	is.Equal(len(report.Findings), 1)
	is.Equal(report.Sources, []string{"test"})
	is.Equal(report.Usage.InputTokens, 2200)
	is.Equal(report.Usage.OutputTokens, 150)
	is.Equal(report.Usage.Provider, "fake")
	is.True(!report.Usage.Truncated)
	is.True(report.DiffSHA256 != "")

	// The system prompt carries the skill, the source docs, and the rules.
	is.True(strings.Contains(provider.system, "Impact Analysis Skill"))
	is.True(strings.Contains(provider.system, "## Data source: test"))
	is.True(strings.Contains(provider.system, "emit_finding"))

	// Source tools and report tools were both registered.
	names := map[string]bool{}
	for _, d := range provider.defs {
		names[d.Name] = true
	}
	is.True(names["test_query"])
	is.True(names["emit_finding"])
	is.True(names["set_summary"])

	// The data source tool ran; its result went back to the model.
	is.Equal(len(src.queries), 1)
	is.Equal(len(provider.sent), 2)
	is.Equal(provider.sent[1].results[0].Content, `[{"lag":41203}]`)
}

func TestAnalyzerFallbackSummaryFromText(t *testing.T) {
	is := is.New(t)
	provider := &fakeProvider{turns: []*Turn{
		{Text: "nothing risky here", StopReason: "end_turn", Usage: TokenUsage{InputTokens: 10, OutputTokens: 10}},
	}}
	a := NewAnalyzer(testConfig(), provider, []DataSource{&fakeSource{}}, nil, nil)
	report, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	is.Equal(report.ChangeSummary, "nothing risky here")
	is.Equal(report.RiskLevel, RiskLow)
	// JSON output always carries arrays, never nulls.
	out, err := report.RenderJSON()
	is.NoErr(err)
	is.True(strings.Contains(string(out), `"findings": []`))
}

func TestAnalyzerTokenBudgetTruncates(t *testing.T) {
	is := is.New(t)
	cfg := testConfig()
	cfg.Agent.TokenBudget = 100
	src := &fakeSource{}
	provider := &fakeProvider{turns: []*Turn{
		{
			ToolCalls: []ToolCall{
				{ID: "1", Name: "test_query", Input: json.RawMessage(`{"sql":"q"}`)},
				{ID: "2", Name: "emit_finding", Input: json.RawMessage(`{"code":"FT_LOSS","summary":"r1","evidence":[{"source":"test","query":"q","value":"v","epoch":"e"}]}`)},
			},
			Usage: TokenUsage{InputTokens: 90, OutputTokens: 20},
		},
		// Never reached: the budget stops the loop before another turn.
	}}
	a := NewAnalyzer(cfg, provider, []DataSource{src}, nil, nil)
	report, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	is.True(report.Usage.Truncated)
	is.Equal(len(provider.sent), 1)
	// The over-budget turn's tool calls still execute: they are local and
	// free, and the final turn is often where the report gets finalized.
	is.Equal(len(src.queries), 1)
	is.Equal(len(report.Findings), 1)
	is.True(strings.Contains(report.RenderMarkdown(), "Partial report"))
}

func TestAnalyzerOutputCapTruncates(t *testing.T) {
	is := is.New(t)
	// A turn with no tool calls but a max_tokens stop is a chopped
	// response, not a clean finish.
	provider := &fakeProvider{turns: []*Turn{
		{Text: "half a sum", StopReason: "max_tokens"},
	}}
	a := NewAnalyzer(testConfig(), provider, []DataSource{&fakeSource{}}, nil, nil)
	report, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	is.True(report.Usage.Truncated)
}

func TestAnalyzerBudgetWarningNearExhaustion(t *testing.T) {
	is := is.New(t)
	cfg := testConfig()
	cfg.Agent.TokenBudget = 1000
	provider := &fakeProvider{turns: []*Turn{
		{
			ToolCalls: []ToolCall{{ID: "1", Name: "test_query", Input: json.RawMessage(`{}`)}},
			// 850 of 1000: past the 80% warning line, under the hard stop.
			Usage: TokenUsage{InputTokens: 800, OutputTokens: 50},
		},
		{Text: "done", StopReason: "end_turn"},
	}}
	a := NewAnalyzer(cfg, provider, []DataSource{&fakeSource{}}, nil, nil)
	_, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	is.Equal(len(provider.sent), 2)
	is.True(strings.Contains(provider.sent[1].text, "budget is nearly exhausted"))
}

func TestAnalyzerUnknownToolSurfacesError(t *testing.T) {
	is := is.New(t)
	provider := &fakeProvider{turns: []*Turn{
		{ToolCalls: []ToolCall{{ID: "1", Name: "nope", Input: json.RawMessage(`{}`)}}},
		{Text: "ok", StopReason: "end_turn"},
	}}
	a := NewAnalyzer(testConfig(), provider, []DataSource{&fakeSource{}}, nil, nil)
	_, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	is.True(provider.sent[1].results[0].IsError)
	is.True(strings.Contains(provider.sent[1].results[0].Content, "unknown tool"))
}

func TestAnalyzerRejectsUnverifiedEvidence(t *testing.T) {
	is := is.New(t)
	src := &fakeSource{}
	provider := &fakeProvider{turns: []*Turn{
		{
			ToolCalls: []ToolCall{
				{ID: "1", Name: "test_query", Input: json.RawMessage(`{"sql":"SELECT lag"}`)},
				// Cites a query that was never executed this run.
				{ID: "2", Name: "emit_finding", Input: json.RawMessage(`{"code":"DATA_LOSS","summary":"s","evidence":[{"source":"test","query":"SELECT bytes FROM somewhere_else","value":"v","epoch":"e"}]}`)},
			},
		},
		{Text: "done", StopReason: "end_turn"},
	}}
	a := NewAnalyzer(testConfig(), provider, []DataSource{src}, nil, nil)
	report, err := a.Run(context.Background(), "diff")
	is.NoErr(err)
	// The finding was rejected and the model was told why.
	is.Equal(len(report.Findings), 0)
	is.True(provider.sent[1].results[1].IsError)
	is.True(strings.Contains(provider.sent[1].results[1].Content, "was not executed"))
}

func TestAnalyzerDiffSeededRepoEvidence(t *testing.T) {
	is := is.New(t)
	// No repo tools attached: file:line citations must still verify against
	// the diff itself, for findings and notes alike.
	provider := &fakeProvider{turns: []*Turn{
		{
			ToolCalls: []ToolCall{
				{ID: "1", Name: "emit_finding", Input: json.RawMessage(`{"code":"UNRESOLVED_ENTITY","summary":"s","evidence":[{"source":"repo","query":"deploy/streams/orders.json:8","value":"max_bytes changed"}]}`)},
				{ID: "2", Name: "emit_note", Input: json.RawMessage(`{"text":"n","evidence":[{"source":"repo","query":"deploy/streams/orders.json:12","value":"replicas changed"}]}`)},
				{ID: "3", Name: "emit_note", Input: json.RawMessage(`{"text":"bad","evidence":[{"source":"repo","query":"deploy/other/file.json:1","value":"v"}]}`)},
			},
		},
		{Text: "done", StopReason: "end_turn"},
	}}
	a := NewAnalyzer(testConfig(), provider, []DataSource{&fakeSource{}}, nil, nil)
	report, err := a.Run(context.Background(), "diff --git a/deploy/streams/orders.json b/deploy/streams/orders.json\n-  \"max_bytes\": 5368709120,\n+  \"max_bytes\": 268435456,")
	is.NoErr(err)
	is.Equal(len(report.Findings), 1)
	is.Equal(len(report.Notes), 1)
	// The note citing a file absent from the diff bounced.
	is.True(provider.sent[1].results[2].IsError)
}

// failingSource fails its health check.
type failingSource struct{ fakeSource }

func (s *failingSource) HealthCheck(context.Context) error { return errors.New("unreachable") }

func TestAnalyzerHealthCheckAborts(t *testing.T) {
	is := is.New(t)
	provider := &fakeProvider{}
	a := NewAnalyzer(testConfig(), provider, []DataSource{&failingSource{}}, nil, nil)
	_, err := a.Run(context.Background(), "diff")
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "health check"))
	// The model was never called.
	is.Equal(len(provider.sent), 0)
}
