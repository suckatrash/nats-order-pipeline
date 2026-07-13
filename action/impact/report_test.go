package impact

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestDeriveRisk(t *testing.T) {
	is := is.New(t)
	is.Equal(DeriveRisk(nil), RiskLow)
	is.Equal(DeriveRisk([]Finding{{Code: CodeUnresolvedEntity}}), RiskLow)
	is.Equal(DeriveRisk([]Finding{{Code: CodeHeadroomExhaustion}}), RiskMedium)
	is.Equal(DeriveRisk([]Finding{{Code: CodeFTLoss}, {Code: CodeHeadroomExhaustion}}), RiskHigh)
	is.Equal(DeriveRisk([]Finding{{Code: CodeBrokenImport}}), RiskHigh)
	is.Equal(DeriveRisk([]Finding{{Code: CodeFTLoss}, {Code: CodeDataLoss}}), RiskCritical)
	is.Equal(DeriveRisk([]Finding{{Code: CodeLimitViolation}}), RiskCritical)
	is.Equal(DeriveRisk([]Finding{{Code: CodeCapacityExceeded}}), RiskCritical)
}

func TestRiskAtLeast(t *testing.T) {
	is := is.New(t)
	is.True(RiskAtLeast(RiskCritical, RiskHigh))
	is.True(RiskAtLeast(RiskHigh, RiskHigh))
	is.True(!RiskAtLeast(RiskMedium, RiskHigh))
	is.True(RiskAtLeast(RiskLow, RiskLow))
	// Unknown threshold never fires.
	is.True(!RiskAtLeast(RiskCritical, "bogus"))
}

func TestRenderMarkdown(t *testing.T) {
	is := is.New(t)
	r := &Report{
		ChangeSummary: "Update stream ORDERS — num_replicas: 3 -> 1",
		RiskLevel:     RiskCritical,
		DataEpoch:     "2026-07-09T14:32:00Z",
		Sources:       []string{"insights"},
		Findings: []Finding{{
			Code:    CodeLimitViolation,
			Summary: "max_bytes (1 GiB) is below current stream size.",
			Evidence: []Evidence{{
				Source: "insights",
				Query:  "SELECT bytes FROM hx.stream_replica_stats WHERE ...",
				Value:  "1.21 GiB",
				Epoch:  "2026-07-09T14:32:00Z",
			}},
		}},
		AffectedEntities: []Entity{{Entity: "order-proc", Type: "consumer", Account: "PROD", Relationship: "consumes ORDERS"}},
		Recommendations:  []string{"Resolve audit-log lag before applying."},
		Notes: []Note{{
			Text:     "consumer.go:89 subscribes with no retry/backoff",
			Evidence: []Evidence{{Source: "repo", Query: "services/audit/consumer.go:89", Value: "js.Subscribe(...)"}},
		}},
		Usage:    Usage{Provider: "anthropic", Model: "claude-opus-4-8", InputTokens: 80_000, OutputTokens: 4_312, Budget: 500_000},
		Duration: "2m14s",
	}
	md := r.RenderMarkdown()
	for _, want := range []string{
		"**Risk: CRITICAL**",
		"data epoch 2026-07-09T14:32:00Z",
		"**LIMIT_VIOLATION**",
		// Evidence collapses: value/epoch/source visible inside the details
		// block, the query beneath in a code fence.
		"   <details><summary>Evidence</summary>",
		"   - 1.21 GiB at epoch 2026-07-09T14:32:00Z (insights)",
		"     SELECT bytes FROM hx.stream_replica_stats WHERE ...",
		"   </details>",
		"| order-proc | consumer | PROD | consumes ORDERS |",
		"1. Resolve audit-log lag",
		"### Notes",
		"  <details><summary>Evidence</summary>",
		"claude-opus-4-8 · 84,312 tokens of 500,000 budget · 2m14s",
	} {
		is.True(strings.Contains(md, want)) // markdown must contain: want
	}
	is.True(!strings.Contains(md, "Partial report"))

	r.Usage.Truncated = true
	is.True(strings.Contains(r.RenderMarkdown(), "Partial report"))
}

func TestRenderMarkdownNoFindings(t *testing.T) {
	is := is.New(t)
	r := &Report{ChangeSummary: "x", RiskLevel: RiskLow}
	md := r.RenderMarkdown()
	is.True(strings.Contains(md, "No catalog findings"))
}
