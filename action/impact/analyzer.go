package impact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Analyzer wires a provider, data sources, and optional repo access into the
// one-shot analysis loop.
type Analyzer struct {
	cfg      *Config
	provider Provider
	sources  []DataSource
	repo     *RepoTools
	log      *slog.Logger
}

// NewAnalyzer builds an Analyzer. repo may be nil (diff + data sources only).
func NewAnalyzer(cfg *Config, provider Provider, sources []DataSource, repo *RepoTools, log *slog.Logger) *Analyzer {
	if log == nil {
		log = slog.Default()
	}
	return &Analyzer{cfg: cfg, provider: provider, sources: sources, repo: repo, log: log}
}

// Run executes the analysis and always returns a Report when the loop ran at
// all: budget exhaustion, the iteration cap, and the run timeout produce a
// truncated report rather than an error. Errors are reserved for failures
// that prevent analysis (unreachable sources, provider errors).
func (a *Analyzer) Run(ctx context.Context, diff string) (*Report, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Agent.Timeout.Std())
	defer cancel()

	// A failing source aborts the run rather than producing a silently
	// under-informed report.
	var sourceDocs []string
	var sourceNames []string
	for _, s := range a.sources {
		if err := s.HealthCheck(ctx); err != nil {
			return nil, fmt.Errorf("data source %s failed health check: %w", s.Name(), err)
		}
		doc, err := s.Describe(ctx)
		if err != nil {
			return nil, fmt.Errorf("data source %s describe: %w", s.Name(), err)
		}
		sourceDocs = append(sourceDocs, doc)
		sourceNames = append(sourceNames, s.Name())
	}

	// Every successful source/repo tool input is recorded so evidence
	// citations can be verified against work actually performed; the diff
	// itself is seeded under "repo" since the model receives it without a
	// tool call.
	evlog := newEvidenceLog()
	evlog.recordText("repo", diff)

	builder := &reportBuilder{verify: evlog.verify}
	tools := map[string]Tool{}
	var defs []ToolDef
	add := func(source string, ts []Tool) {
		for _, t := range ts {
			if source != "" {
				t.Handler = recordingHandler(evlog, source, t.Handler)
			}
			tools[t.Def.Name] = t
			defs = append(defs, t.Def)
		}
	}
	for _, s := range a.sources {
		add(s.Name(), s.Tools())
	}
	repoDoc := ""
	if a.repo != nil {
		add("repo", a.repo.Tools())
		repoDoc = a.repo.Describe()
	}
	add("", builder.Tools())

	system := buildSystemPrompt(sourceDocs, repoDoc, a.cfg)
	conv := a.provider.Start(system, defs)

	var usage TokenUsage
	budget := a.cfg.Agent.TokenBudget
	finished := false
	lastText := ""
	warned := false

	userText := initialUserMessage(diff)
	var results []ToolResult

loop:
	for iter := range a.cfg.Agent.MaxIterations {
		turn, err := conv.Send(ctx, userText, results)
		if err != nil {
			// The run deadline and an operator interrupt both produce a
			// partial report, not a failure — unless nothing came back yet.
			if (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && iter > 0 {
				a.log.Warn("run interrupted; finalizing partial report", "cause", err)
				break loop
			}
			return nil, fmt.Errorf("model turn %d: %w", iter+1, err)
		}
		usage.InputTokens += turn.Usage.InputTokens
		usage.OutputTokens += turn.Usage.OutputTokens
		if turn.Text != "" {
			lastText = turn.Text
		}
		a.log.Info("turn complete",
			"turn", iter+1,
			"tool_calls", len(turn.ToolCalls),
			"stop_reason", turn.StopReason,
			"tokens", usage.InputTokens+usage.OutputTokens,
		)

		if len(turn.ToolCalls) == 0 {
			// A response cut off by the output cap is not a clean finish —
			// its text is a chopped fallback summary at best.
			finished = turn.StopReason != "max_tokens" && turn.StopReason != "length"
			break loop
		}

		// Execute tool calls before any budget decision: report tools are
		// local and free, and a model spending its last turn finalizing the
		// report must not have those calls dropped.
		results = results[:0]
		for _, call := range turn.ToolCalls {
			tool, ok := tools[call.Name]
			var content string
			var isErr bool
			if !ok {
				content, isErr = fmt.Sprintf("unknown tool: %s", call.Name), true
			} else {
				content, isErr = tool.Handler(ctx, call.Input)
			}
			a.log.Info("tool executed", "name", call.Name, "error", isErr, "result_bytes", len(content))
			results = append(results, ToolResult{ID: call.ID, Content: truncateResult(content), IsError: isErr})
		}

		// The hard budget stop sits between tool execution and the next
		// model round-trip: everything already earned lands in the report,
		// but no further tokens are spent.
		spent := usage.InputTokens + usage.OutputTokens
		if budget > 0 && spent >= budget {
			a.log.Warn("token budget exhausted; finalizing partial report", "spent", spent, "budget", budget)
			break loop
		}

		// Past 80% of the budget, tell the model once to wrap up so the hard
		// stop above lands on a finalized report instead of mid-investigation.
		userText = ""
		if budget > 0 && !warned && spent >= budget*8/10 {
			warned = true
			userText = "Note: the token budget is nearly exhausted. Stop investigating; emit any findings already substantiated and call set_summary now."
		}
	}

	report := builder.report
	if report.ChangeSummary == "" {
		// The model never called set_summary; fall back to its final text so
		// the report is never headless.
		report.ChangeSummary = lastText
	}
	report.RiskLevel = DeriveRisk(report.Findings)
	report.Sources = sourceNames
	report.Usage = Usage{
		Provider:     a.provider.Name(),
		Model:        a.cfg.Agent.Model,
		InputTokens:  usage.InputTokens,
		OutputTokens: usage.OutputTokens,
		Budget:       budget,
		Truncated:    !finished,
	}
	report.Duration = FormatDuration(time.Since(start))
	sum := sha256.Sum256([]byte(diff))
	report.DiffSHA256 = hex.EncodeToString(sum[:])
	// Normalize nil slices so the JSON output always carries arrays.
	if report.Findings == nil {
		report.Findings = []Finding{}
	}
	if report.AffectedEntities == nil {
		report.AffectedEntities = []Entity{}
	}
	if report.Recommendations == nil {
		report.Recommendations = []string{}
	}
	if report.Notes == nil {
		report.Notes = []Note{}
	}
	return &report, nil
}
