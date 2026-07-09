package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Insights query API subjects. These mirror the constants in the api package
// (api/service.go); they are redeclared here so the impact binary does not
// import the server side of the repo (and its CGO DuckDB dependency).
const (
	insightsQuerySubject   = "$INS.db.query"
	insightsSchemasSubject = "$INS.db.schemas"
	insightsTablesSubject  = "$INS.db.tables"
	insightsColumnsSubject = "$INS.db.columns"
	insightsMacrosSubject  = "$INS.db.macros"
)

// natsRequester is the slice of *nats.Conn the source uses; tests substitute
// a stub.
type natsRequester interface {
	RequestWithContext(ctx context.Context, subj string, data []byte) (*nats.Msg, error)
}

// insightsSource exposes the Insights query API (NATS micro request/reply)
// as agent tools. The server side enforces read-only statements, row limits,
// and query timeouts — this side is a thin client.
type insightsSource struct {
	nc      natsRequester
	timeout time.Duration
}

// ConnectInsights dials the configured NATS endpoint and returns the source
// plus a close function.
func ConnectInsights(cfg *InsightsConfig) (DataSource, func(), error) {
	opts := []nats.Option{
		nats.Name("impact-analysis"),
		nats.Timeout(10 * time.Second),
	}
	if cfg.Creds != "" {
		opts = append(opts, nats.UserCredentials(cfg.Creds))
	}
	nc, err := nats.Connect(cfg.Endpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to insights endpoint: %w", err)
	}
	return &insightsSource{nc: nc, timeout: 30 * time.Second}, nc.Close, nil
}

func (s *insightsSource) Name() string { return "insights" }

func (s *insightsSource) Describe(ctx context.Context) (string, error) {
	return `## Data source: insights

The insights_* tools query the Insights operational database (DuckDB) over
NATS. This is the primary source for entity state, metrics, configuration,
relationships, and audit check results.

- Use discovery first: insights_schemas, insights_tables(schema),
  insights_columns(schema, table), insights_macros(schema).
- Always qualify table names with the schema prefix (e.g. hx.stream_ident).
- insights_query executes a single read-only SQL statement and returns a JSON
  array of row objects. Results are row-limited server-side; add LIMIT to
  large scans.
- Cite evidence from this source as source "insights" with the SQL query, the
  value observed, and the epoch it came from.`, nil
}

func (s *insightsSource) HealthCheck(ctx context.Context) error {
	msg, isErr := s.request(ctx, insightsSchemasSubject, []byte(`{}`))
	if isErr {
		return fmt.Errorf("insights query API health check on %s: %s", insightsSchemasSubject, msg)
	}
	return nil
}

func (s *insightsSource) Tools() []Tool {
	pass := func(subject string) func(context.Context, json.RawMessage) (string, bool) {
		return func(ctx context.Context, input json.RawMessage) (string, bool) {
			payload := []byte(input)
			if len(payload) == 0 {
				payload = []byte(`{}`)
			}
			return s.request(ctx, subject, payload)
		}
	}
	return []Tool{
		{
			Def: ToolDef{
				Name:        "insights_query",
				Description: "Execute a single read-only SQL statement against the Insights database. Returns a JSON array of row objects. Qualify tables with their schema (e.g. hx.stream_ident).",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"sql":{"type":"string","description":"The SQL statement to execute"}},"required":["sql"]}`),
			},
			Handler: pass(insightsQuerySubject),
		},
		{
			Def: ToolDef{
				Name:        "insights_schemas",
				Description: "List the queryable Insights database schemas with descriptions.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
			},
			Handler: pass(insightsSchemasSubject),
		},
		{
			Def: ToolDef{
				Name:        "insights_tables",
				Description: "List tables and views in an Insights schema with descriptions.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string","description":"Schema name (e.g. hx, audit)"}},"required":["schema"]}`),
			},
			Handler: pass(insightsTablesSubject),
		},
		{
			Def: ToolDef{
				Name:        "insights_columns",
				Description: "List columns of an Insights table or view with types and descriptions.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string"},"table":{"type":"string"}},"required":["schema","table"]}`),
			},
			Handler: pass(insightsColumnsSubject),
		},
		{
			Def: ToolDef{
				Name:        "insights_macros",
				Description: "List Insights SQL macros (audit checks) with signatures and descriptions.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string","description":"Schema name (e.g. audit)"}},"required":["schema"]}`),
			},
			Handler: pass(insightsMacrosSubject),
		},
	}
}

// request performs one NATS request and maps micro service errors to tool
// errors the model can react to.
func (s *insightsSource) request(ctx context.Context, subject string, payload []byte) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	msg, err := s.nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return fmt.Sprintf("NATS request error: %v", err), true
	}
	if errMsg := msg.Header.Get("Nats-Service-Error"); errMsg != "" {
		code := msg.Header.Get("Nats-Service-Error-Code")
		return fmt.Sprintf("Insights API error (code %s): %s", code, errMsg), true
	}
	return truncateResult(string(msg.Data)), false
}
