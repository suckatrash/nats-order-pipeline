package impact

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/matryer/is"
)

// promServer fakes the Prometheus HTTP API, recording the last request for
// assertions.
func promServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *prometheusSource) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)
	src, err := ConnectPrometheus(&PrometheusConfig{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return srv, src.(*prometheusSource)
}

func TestPrometheusQueryReturnsData(t *testing.T) {
	is := is.New(t)
	var gotPath, gotQuery string
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query().Get("query")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"pod":"minimal-nats-0"},"value":[1719000000,"42"]}]}}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_query", `{"query":"up{namespace=\"nats\"}"}`)
	is.True(!isErr)
	is.Equal(gotPath, "/api/v1/query")
	is.Equal(gotQuery, `up{namespace="nats"}`)
	// Only the data payload is returned; the status envelope is stripped.
	is.Equal(out, `{"resultType":"vector","result":[{"metric":{"pod":"minimal-nats-0"},"value":[1719000000,"42"]}]}`)
}

func TestPrometheusQueryRangeForwardsWindow(t *testing.T) {
	is := is.New(t)
	var got url.Values
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		w.Write([]byte(`{"status":"success","data":{}}`))
	})

	_, isErr := callTool(t, src.Tools(), "prometheus_query_range", `{"query":"up"}`)
	is.True(isErr) // missing start/end/step

	out, isErr := callTool(t, src.Tools(), "prometheus_query_range",
		`{"query":"rate(nats_varz_in_bytes[5m])","start":"2026-07-10T00:00:00Z","end":"2026-07-10T01:00:00Z","step":"1m"}`)
	is.True(!isErr)
	is.Equal(out, `{}`)
	is.Equal(got.Get("query"), "rate(nats_varz_in_bytes[5m])")
	is.Equal(got.Get("start"), "2026-07-10T00:00:00Z")
	is.Equal(got.Get("end"), "2026-07-10T01:00:00Z")
	is.Equal(got.Get("step"), "1m")
}

func TestPrometheusAPIErrorIsToolError(t *testing.T) {
	is := is.New(t)
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error at char 3"}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_query", `{"query":"up{"}`)
	is.True(isErr)
	is.True(len(out) > 0)
	is.Equal(out, "prometheus API error (bad_data): parse error at char 3")
}

func TestPrometheusNonJSONErrorSurfacesStatus(t *testing.T) {
	is := is.New(t)
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`<html>401 Authorization Required</html>`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_query", `{"query":"up"}`)
	is.True(isErr)
	is.Equal(out, "prometheus HTTP 401: <html>401 Authorization Required</html>")
}

func TestPrometheusBasicAuthHeader(t *testing.T) {
	is := is.New(t)
	var user, pass string
	var withAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, withAuth = r.BasicAuth()
		w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	t.Cleanup(srv.Close)
	src, err := ConnectPrometheus(&PrometheusConfig{URL: srv.URL, Username: "impact", Password: "s3cret"})
	is.NoErr(err)

	is.NoErr(src.HealthCheck(context.Background()))
	is.True(withAuth)
	is.Equal(user, "impact")
	is.Equal(pass, "s3cret")
}

func TestPrometheusMetricsDiscoveryMatch(t *testing.T) {
	is := is.New(t)
	var gotMatch string
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotMatch = r.URL.Query().Get("match[]")
		w.Write([]byte(`{"status":"success","data":["nats_stream_bytes","nats_stream_msgs"]}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_metrics", `{"match":"nats_stream_.+"}`)
	is.True(!isErr)
	is.Equal(gotMatch, `{__name__=~"nats_stream_.+"}`)
	is.Equal(out, `["nats_stream_bytes","nats_stream_msgs"]`)
}

func TestPrometheusProxyErrorSurfacesHTTPStatus(t *testing.T) {
	is := is.New(t)
	// JSON that parses but is not the Prometheus envelope (e.g. an ingress
	// error page) must surface the HTTP status, not an empty API error.
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"message":"bad gateway"}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_query", `{"query":"up"}`)
	is.True(isErr)
	is.Equal(out, `prometheus HTTP 502: {"message":"bad gateway"}`)
}

func TestPrometheusOversizedResultIsActionableError(t *testing.T) {
	is := is.New(t)
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":["`))
		filler := make([]byte, 1<<16)
		for i := range filler {
			filler[i] = 'x'
		}
		for written := 0; written <= maxAPIBodyBytes; written += len(filler) {
			w.Write(filler)
		}
		w.Write([]byte(`"]}}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_query_range",
		`{"query":"up","start":"1","end":"2","step":"1m"}`)
	is.True(isErr)
	// Short and actionable — not a truncated JSON dump.
	is.True(len(out) < 200)
	is.True(strings.Contains(out, "too large"))
}

func TestPrometheusLabelsListsValues(t *testing.T) {
	is := is.New(t)
	var gotPath string
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"status":"success","data":["nats","order-pipeline"]}`))
	})

	out, isErr := callTool(t, src.Tools(), "prometheus_labels", `{"label":"namespace"}`)
	is.True(!isErr)
	is.Equal(gotPath, "/api/v1/label/namespace/values")
	is.Equal(out, `["nats","order-pipeline"]`)

	_, isErr = callTool(t, src.Tools(), "prometheus_labels", `{}`)
	is.True(isErr) // label is required
}

func TestConnectPrometheusRejectsBadURL(t *testing.T) {
	is := is.New(t)
	_, err := ConnectPrometheus(&PrometheusConfig{URL: "not-a-url"})
	is.True(err != nil)
}

// The PromQL expression is recorded under the citable "query" key, so
// evidence citing it verifies; discovery inputs (match filters) do not.
func TestPrometheusQueryIsCitableEvidence(t *testing.T) {
	is := is.New(t)
	_, src := promServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	})

	evlog := newEvidenceLog()
	tools := src.Tools()
	for i := range tools {
		tools[i].Handler = recordingHandler(evlog, src.Name(), tools[i].Def, tools[i].Handler)
	}
	_, isErr := callTool(t, tools, "prometheus_query", `{"query":"sum(node_filesystem_avail_bytes)"}`)
	is.True(!isErr)
	_, isErr = callTool(t, tools, "prometheus_metrics", `{"match":"node_.+"}`)
	is.True(!isErr)

	is.NoErr(evlog.verify(Evidence{Source: "prometheus", Query: "sum(node_filesystem_avail_bytes)"}))
	// A metric-name filter seen during discovery is not citable work.
	is.True(evlog.verify(Evidence{Source: "prometheus", Query: "node_.+"}) != nil)
	// A never-executed expression does not verify.
	is.True(evlog.verify(Evidence{Source: "prometheus", Query: "sum(node_memory_MemAvailable_bytes)"}) != nil)
}
