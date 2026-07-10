package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// prometheusSource exposes a Prometheus HTTP API as agent tools. It covers
// the layer beneath NATS — node and filesystem capacity, Kubernetes object
// state, and exporter metrics — that the Insights database cannot see.
type prometheusSource struct {
	client   *http.Client
	baseURL  string
	username string
	password string
}

// ConnectPrometheus builds the source. The URL is validated here; actual
// reachability is the analyzer's HealthCheck.
func ConnectPrometheus(cfg *PrometheusConfig) (DataSource, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid prometheus url %q: %w", cfg.URL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid prometheus url %q: scheme and host are required", cfg.URL)
	}
	return &prometheusSource{
		client:   &http.Client{Timeout: 30 * time.Second},
		baseURL:  strings.TrimRight(cfg.URL, "/"),
		username: cfg.Username,
		password: cfg.Password,
	}, nil
}

func (s *prometheusSource) Name() string { return "prometheus" }

func (s *prometheusSource) Describe(ctx context.Context) (string, error) {
	return `## Data source: prometheus

The prometheus_* tools query a Prometheus server scraping the environment the
change deploys into. Use it for the layer Insights cannot see: node and
filesystem capacity, Kubernetes object state (pods, statefulsets, PVCs,
resource requests/limits), and per-process metrics from exporters.

- The metric inventory varies by environment. Discover before querying:
  prometheus_metrics(match) lists metric names; prometheus_labels(label,
  match) lists the values of a label (e.g. namespace, pod) so entity names
  are observed, not guessed.
- Kubernetes state (when kube-state-metrics is scraped) links infrastructure
  identifiers to workloads: kube_pod_info, kube_statefulset_replicas,
  kube_persistentvolumeclaim_resource_requests_storage_bytes. Node capacity
  (when node-exporter is scraped) lives in node_filesystem_avail_bytes,
  node_memory_MemAvailable_bytes.
- prometheus_query evaluates one instant PromQL expression;
  prometheus_query_range evaluates over a window for trends (rate of growth,
  headroom projection).
- To use a label correlation as entity-binding evidence (e.g. a pod name that
  matches a NATS server name in Insights), confirm it with a prometheus_query
  expression and cite that expression — discovery-tool inputs are not citable.
- Cite evidence from this source as source "prometheus" with the exact PromQL
  expression executed, the value observed, and its timestamp.`, nil
}

func (s *prometheusSource) HealthCheck(ctx context.Context) error {
	msg, isErr := s.get(ctx, "/api/v1/status/buildinfo", nil)
	if isErr {
		return fmt.Errorf("prometheus health check on %s: %s", s.baseURL, msg)
	}
	return nil
}

func (s *prometheusSource) Tools() []Tool {
	return []Tool{
		{
			Def: ToolDef{
				Name:        "prometheus_query",
				Description: "Evaluate one instant PromQL expression. Returns the Prometheus API result JSON (resultType and result series with labels, value, timestamp).",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"The PromQL expression to evaluate"},"time":{"type":"string","description":"Optional evaluation timestamp (RFC3339 or unix seconds); defaults to now"}},"required":["query"]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Query string `json:"query"`
					Time  string `json:"time"`
				}
				if err := json.Unmarshal(input, &in); err != nil || in.Query == "" {
					return "invalid input: a query string is required", true
				}
				params := url.Values{"query": {in.Query}}
				if in.Time != "" {
					params.Set("time", in.Time)
				}
				return s.get(ctx, "/api/v1/query", params)
			},
		},
		{
			Def: ToolDef{
				Name:        "prometheus_query_range",
				Description: "Evaluate a PromQL expression over a time window. Use for trends: growth rates, headroom projections, before/after comparisons.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"The PromQL expression to evaluate"},"start":{"type":"string","description":"Window start (RFC3339 or unix seconds)"},"end":{"type":"string","description":"Window end (RFC3339 or unix seconds)"},"step":{"type":"string","description":"Resolution step (e.g. 1m, 5m)"}},"required":["query","start","end","step"]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Query string `json:"query"`
					Start string `json:"start"`
					End   string `json:"end"`
					Step  string `json:"step"`
				}
				if err := json.Unmarshal(input, &in); err != nil || in.Query == "" || in.Start == "" || in.End == "" || in.Step == "" {
					return "invalid input: query, start, end, and step are required", true
				}
				return s.get(ctx, "/api/v1/query_range", url.Values{
					"query": {in.Query}, "start": {in.Start}, "end": {in.End}, "step": {in.Step},
				})
			},
		},
		{
			Def: ToolDef{
				Name:        "prometheus_metrics",
				Description: "List metric names, optionally filtered by a RE2 regular expression. Use to discover what this environment's Prometheus actually scrapes before writing queries.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"match":{"type":"string","description":"Optional RE2 regexp; only metric names matching it are returned (e.g. nats_.+, kube_pod_.+)"}},"required":[]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Match string `json:"match"`
				}
				if err := json.Unmarshal(input, &in); err != nil {
					return "invalid input", true
				}
				params := url.Values{}
				if in.Match != "" {
					params.Set("match[]", fmt.Sprintf("{__name__=~%q}", in.Match))
				}
				return s.get(ctx, "/api/v1/label/__name__/values", params)
			},
		},
		{
			Def: ToolDef{
				Name:        "prometheus_labels",
				Description: "List the values of one label, optionally restricted to a series selector. Use to enumerate live entities (namespaces, pods, statefulsets) instead of guessing names.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"label":{"type":"string","description":"Label name (e.g. namespace, pod, job)"},"match":{"type":"string","description":"Optional series selector (e.g. kube_pod_info{namespace=\"nats\"})"}},"required":["label"]}`),
			},
			Handler: func(ctx context.Context, input json.RawMessage) (string, bool) {
				var in struct {
					Label string `json:"label"`
					Match string `json:"match"`
				}
				if err := json.Unmarshal(input, &in); err != nil || in.Label == "" {
					return "invalid input: a label name is required", true
				}
				params := url.Values{}
				if in.Match != "" {
					params.Set("match[]", in.Match)
				}
				return s.get(ctx, "/api/v1/label/"+url.PathEscape(in.Label)+"/values", params)
			},
		},
	}
}

// maxAPIBodyBytes is the sanity cap on a Prometheus response body — large
// enough to parse any reasonable result before truncateResult trims it for
// the model, small enough that a runaway query cannot exhaust memory.
const maxAPIBodyBytes = 4 << 20

// errBodyMax caps how much of an error body is echoed to the model.
const errBodyMax = 2048

func errBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > errBodyMax {
		s = s[:errBodyMax] + "..."
	}
	return s
}

// get performs one Prometheus API request and maps HTTP and API-level errors
// to tool errors the model can react to. On success it returns the "data"
// payload; the status envelope carries no information worth the tokens.
func (s *prometheusSource) get(ctx context.Context, path string, params url.Values) (string, bool) {
	u := s.baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Sprintf("build request: %v", err), true
	}
	if s.username != "" {
		req.SetBasicAuth(s.username, s.password)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Sprintf("prometheus request error: %v", err), true
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIBodyBytes+1))
	if err != nil {
		return fmt.Sprintf("read prometheus response: %v", err), true
	}
	if len(body) > maxAPIBodyBytes {
		return "result too large — aggregate (sum, count, topk) or narrow the selector and time window", true
	}
	var envelope struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		if resp.StatusCode != http.StatusOK {
			return fmt.Sprintf("prometheus HTTP %d: %s", resp.StatusCode, errBody(body)), true
		}
		return fmt.Sprintf("unexpected prometheus response: %s", errBody(body)), true
	}
	if envelope.Status != "success" {
		// A proxy or ingress can answer with JSON that parses but is not the
		// Prometheus envelope; without its error field the HTTP status is the
		// only signal.
		if envelope.Error == "" {
			return fmt.Sprintf("prometheus HTTP %d: %s", resp.StatusCode, errBody(body)), true
		}
		return fmt.Sprintf("prometheus API error (%s): %s", envelope.ErrorType, envelope.Error), true
	}
	return truncateResult(string(envelope.Data)), false
}
