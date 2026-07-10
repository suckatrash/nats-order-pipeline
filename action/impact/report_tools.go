package impact

import (
	"context"
	"encoding/json"
	"fmt"
)

// reportBuilder accumulates the report as the agent emits structured pieces
// through tools. Tool-side validation (catalog codes, required evidence) is
// what makes the findings policy structural rather than advisory: an
// unsubstantiated finding is rejected at the tool boundary and the model is
// told why.
type reportBuilder struct {
	report Report
	// verify, when set, re-checks each evidence citation against the run's
	// execution log; unverifiable evidence rejects the finding or note.
	verify func(Evidence) error
}

// evidenceSchema is shared by emit_finding and emit_note.
const evidenceSchema = `{
	"type": "array",
	"minItems": 1,
	"items": {
		"type": "object",
		"properties": {
			"source": {"type": "string", "description": "Data source name (e.g. insights) or 'repo'"},
			"query": {"type": "string", "description": "The SQL query, or file:line for repo observations"},
			"value": {"type": "string", "description": "The observed value that substantiates the claim"},
			"epoch": {"type": "string", "description": "Epoch timestamp of the data (omit for repo observations)"}
		},
		"required": ["source", "query", "value"]
	}
}`

func (rb *reportBuilder) Tools() []Tool {
	findingSchema := fmt.Sprintf(`{
		"type": "object",
		"properties": {
			"code": {"type": "string", "enum": ["%s","%s","%s","%s","%s","%s","%s"]},
			"summary": {"type": "string", "description": "One-sentence statement of the finding"},
			"evidence": %s
		},
		"required": ["code", "summary", "evidence"]
	}`, CodeDataLoss, CodeLimitViolation, CodeHeadroomExhaustion, CodeFTLoss,
		CodeCapacityExceeded, CodeBrokenImport, CodeUnresolvedEntity, evidenceSchema)

	noteSchema := fmt.Sprintf(`{
		"type": "object",
		"properties": {
			"text": {"type": "string"},
			"evidence": %s
		},
		"required": ["text", "evidence"]
	}`, evidenceSchema)

	return []Tool{
		{
			Def: ToolDef{
				Name:        "emit_finding",
				Description: "Record one catalog finding. Only use when the required evidence for the code is present in data you actually queried — every finding must cite concrete measurements.",
				InputSchema: json.RawMessage(findingSchema),
			},
			Handler: rb.emitFinding,
		},
		{
			Def: ToolDef{
				Name:        "emit_note",
				Description: "Record a non-scored observation directly related to a changed entity. Requires evidence like a finding (file:line for repo observations; query, value, and epoch for data observations).",
				InputSchema: json.RawMessage(noteSchema),
			},
			Handler: rb.emitNote,
		},
		{
			Def: ToolDef{
				Name:        "set_summary",
				Description: "Set the report's change summary, data epoch, affected entities, and recommendations. Call once near the end of the analysis; calling again replaces the previous values.",
				InputSchema: json.RawMessage(`{
					"type": "object",
					"properties": {
						"change_summary": {"type": "string", "description": "One-line interpretation of the change"},
						"data_epoch": {"type": "string", "description": "The latest epoch the analysis ran against"},
						"affected_entities": {
							"type": "array",
							"items": {
								"type": "object",
								"properties": {
									"entity": {"type": "string"},
									"type": {"type": "string"},
									"account": {"type": "string"},
									"relationship": {"type": "string", "description": "How the entity is affected (e.g. 'consumes ORDERS')"}
								},
								"required": ["entity", "type", "relationship"]
							}
						},
						"recommendations": {
							"type": "array",
							"items": {"type": "string"},
							"description": "Actionable recommendations, each tied to a finding"
						}
					},
					"required": ["change_summary"]
				}`),
			},
			Handler: rb.setSummary,
		},
	}
}

func (rb *reportBuilder) validEvidence(ev []Evidence) error {
	if len(ev) == 0 {
		return fmt.Errorf("evidence is required: cite the query, the observed value, and (for data sources) the epoch")
	}
	for _, e := range ev {
		if e.Source == "" || e.Query == "" || e.Value == "" {
			return fmt.Errorf("each evidence entry needs source, query, and value")
		}
		if rb.verify != nil {
			if err := rb.verify(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rb *reportBuilder) emitFinding(_ context.Context, input json.RawMessage) (string, bool) {
	var f Finding
	if err := json.Unmarshal(input, &f); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	if _, ok := findingRisk[f.Code]; !ok {
		return fmt.Sprintf("unknown finding code %q — findings are restricted to the fixed catalog", f.Code), true
	}
	if f.Summary == "" {
		return "summary is required", true
	}
	if err := rb.validEvidence(f.Evidence); err != nil {
		return err.Error(), true
	}
	rb.report.Findings = append(rb.report.Findings, f)
	return fmt.Sprintf("finding %d recorded (%s)", len(rb.report.Findings), f.Code), false
}

func (rb *reportBuilder) emitNote(_ context.Context, input json.RawMessage) (string, bool) {
	var n Note
	if err := json.Unmarshal(input, &n); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	if n.Text == "" {
		return "text is required", true
	}
	if err := rb.validEvidence(n.Evidence); err != nil {
		return err.Error(), true
	}
	rb.report.Notes = append(rb.report.Notes, n)
	return fmt.Sprintf("note %d recorded", len(rb.report.Notes)), false
}

func (rb *reportBuilder) setSummary(_ context.Context, input json.RawMessage) (string, bool) {
	var params struct {
		ChangeSummary    string   `json:"change_summary"`
		DataEpoch        string   `json:"data_epoch"`
		AffectedEntities []Entity `json:"affected_entities"`
		Recommendations  []string `json:"recommendations"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}
	if params.ChangeSummary == "" {
		return "change_summary is required", true
	}
	rb.report.ChangeSummary = params.ChangeSummary
	rb.report.DataEpoch = params.DataEpoch
	rb.report.AffectedEntities = params.AffectedEntities
	rb.report.Recommendations = params.Recommendations
	return "summary set", false
}
