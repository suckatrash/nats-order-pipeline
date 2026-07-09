package impact

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Finding codes form the fixed catalog: the agent may only emit findings with
// these codes, and each maps to a fixed severity so the headline risk level
// is derived mechanically, never assigned by model judgment.
const (
	CodeDataLoss           = "DATA_LOSS"
	CodeLimitViolation     = "LIMIT_VIOLATION"
	CodeHeadroomExhaustion = "HEADROOM_EXHAUSTION"
	CodeFTLoss             = "FT_LOSS"
	CodeCapacityExceeded   = "CAPACITY_EXCEEDED"
	CodeBrokenImport       = "BROKEN_IMPORT"
	CodeUnresolvedEntity   = "UNRESOLVED_ENTITY"
)

// Risk levels, in ascending order.
const (
	RiskLow      = "low"
	RiskMedium   = "medium"
	RiskHigh     = "high"
	RiskCritical = "critical"
)

// findingRisk ranks each catalog code. UNRESOLVED_ENTITY is 0: it must be
// prominent in the report but does not raise the risk level.
var findingRisk = map[string]int{
	CodeDataLoss:           3,
	CodeLimitViolation:     3,
	CodeCapacityExceeded:   3,
	CodeFTLoss:             2,
	CodeBrokenImport:       2,
	CodeHeadroomExhaustion: 1,
	CodeUnresolvedEntity:   0,
}

var riskNames = [...]string{RiskLow, RiskMedium, RiskHigh, RiskCritical}

// riskRank orders levels for threshold comparison. Unknown levels rank -1.
func riskRank(level string) int {
	for i, n := range riskNames {
		if n == level {
			return i
		}
	}
	return -1
}

// RiskAtLeast reports whether level meets threshold — the --fail-on gate.
func RiskAtLeast(level, threshold string) bool {
	return riskRank(level) >= riskRank(threshold) && riskRank(threshold) >= 0
}

// DeriveRisk maps findings to the headline risk level: the maximum severity
// across emitted finding codes, low when there are none.
func DeriveRisk(findings []Finding) string {
	maxRank := 0
	for _, f := range findings {
		if r := findingRisk[f.Code]; r > maxRank {
			maxRank = r
		}
	}
	return riskNames[maxRank]
}

// Evidence is one verifiable measurement or observation backing a finding or
// note: what was asked, of which source, what came back, and how fresh it is.
// Repo observations put the file:line reference in Query and leave Epoch empty.
type Evidence struct {
	Source string `json:"source"`
	Query  string `json:"query"`
	Value  string `json:"value"`
	Epoch  string `json:"epoch,omitempty"`
}

// Finding is one catalog finding with its supporting evidence.
type Finding struct {
	Code     string     `json:"code"`
	Summary  string     `json:"summary"`
	Evidence []Evidence `json:"evidence"`
}

// Note is a non-scored observation directly related to a changed entity. It
// carries evidence exactly like a finding.
type Note struct {
	Text     string     `json:"text"`
	Evidence []Evidence `json:"evidence"`
}

// Entity is one row of the affected-entities table.
type Entity struct {
	Entity       string `json:"entity"`
	Type         string `json:"type"`
	Account      string `json:"account,omitempty"`
	Relationship string `json:"relationship"`
}

// Usage records the run's token spend against its budget.
type Usage struct {
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
	Budget       int    `json:"budget"`
	// Truncated is set when the run stopped on the token budget or the
	// iteration cap rather than the agent finishing.
	Truncated bool `json:"truncated"`
}

// Report is the analysis result. It marshals directly as the --format json
// output; RenderMarkdown produces the human form.
type Report struct {
	ChangeSummary    string    `json:"change_summary"`
	RiskLevel        string    `json:"risk_level"`
	DataEpoch        string    `json:"data_epoch,omitempty"`
	Sources          []string  `json:"sources"`
	Findings         []Finding `json:"findings"`
	AffectedEntities []Entity  `json:"affected_entities"`
	Recommendations  []string  `json:"recommendations"`
	Notes            []Note    `json:"notes"`
	Usage            Usage     `json:"usage"`
	Duration         string    `json:"duration"`
	DiffSHA256       string    `json:"diff_sha256"`
}

// RenderJSON returns the stable machine-readable form.
func (r *Report) RenderJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// RenderMarkdown returns the human report. Layout follows the design doc: the
// headline risk is the mechanical derivation, findings lead, notes trail, and
// the footer carries token usage.
func (r *Report) RenderMarkdown() string {
	var b strings.Builder
	b.WriteString("## Impact Analysis\n\n")
	fmt.Fprintf(&b, "**Change:** %s\n", strings.TrimSpace(r.ChangeSummary))
	fmt.Fprintf(&b, "**Risk: %s**", strings.ToUpper(r.RiskLevel))
	if r.DataEpoch != "" {
		fmt.Fprintf(&b, " · data epoch %s", r.DataEpoch)
	}
	if len(r.Sources) > 0 {
		fmt.Fprintf(&b, " · sources: %s", strings.Join(r.Sources, ", "))
	}
	b.WriteString("\n")
	if r.Usage.Truncated {
		b.WriteString("\n> **Partial report:** the run stopped at its token or iteration budget before the agent finished.\n")
	}

	b.WriteString("\n### Findings\n\n")
	if len(r.Findings) == 0 {
		b.WriteString("No catalog findings — current operational data does not substantiate the risk conditions this analysis checks for.\n")
	}
	for i, f := range r.Findings {
		fmt.Fprintf(&b, "%d. **%s** — %s\n", i+1, f.Code, f.Summary)
		for _, e := range f.Evidence {
			b.WriteString("   Evidence: " + e.render() + "\n")
		}
	}

	if len(r.AffectedEntities) > 0 {
		b.WriteString("\n### Affected Entities\n\n")
		b.WriteString("| Entity | Type | Account | Relationship |\n|---|---|---|---|\n")
		for _, e := range r.AffectedEntities {
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", e.Entity, e.Type, e.Account, e.Relationship)
		}
	}

	if len(r.Recommendations) > 0 {
		b.WriteString("\n### Recommendations\n\n")
		for i, rec := range r.Recommendations {
			fmt.Fprintf(&b, "%d. %s\n", i+1, rec)
		}
	}

	if len(r.Notes) > 0 {
		b.WriteString("\n### Notes\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n.Text)
			for _, e := range n.Evidence {
				b.WriteString("  Evidence: " + e.render() + "\n")
			}
		}
	}

	fmt.Fprintf(&b, "\n---\n%s · %s tokens of %s budget · %s\n",
		r.Usage.Model,
		formatInt(r.Usage.InputTokens+r.Usage.OutputTokens),
		formatInt(r.Usage.Budget),
		r.Duration,
	)
	return b.String()
}

func (e Evidence) render() string {
	parts := []string{}
	if e.Query != "" {
		parts = append(parts, e.Query)
	}
	if e.Value != "" {
		parts = append(parts, "= "+e.Value)
	}
	if e.Epoch != "" {
		parts = append(parts, "at epoch "+e.Epoch)
	}
	if e.Source != "" {
		parts = append(parts, "("+e.Source+")")
	}
	return strings.Join(parts, " ")
}

// formatInt renders n with thousands separators for the report footer.
func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

// FormatDuration renders a run duration for the report (whole seconds).
func FormatDuration(d time.Duration) string {
	return d.Round(time.Second).String()
}
