package impact

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
)

// stubRequester scripts NATS replies per subject.
type stubRequester struct {
	replies map[string]*nats.Msg
	err     error
	// last records the most recent request for assertions.
	lastSubject string
	lastPayload []byte
}

func (s *stubRequester) RequestWithContext(_ context.Context, subj string, data []byte) (*nats.Msg, error) {
	s.lastSubject = subj
	s.lastPayload = data
	if s.err != nil {
		return nil, s.err
	}
	if msg, ok := s.replies[subj]; ok {
		return msg, nil
	}
	return &nats.Msg{Data: []byte(`[]`), Header: nats.Header{}}, nil
}

func TestInsightsSourceQueryPassthrough(t *testing.T) {
	is := is.New(t)
	stub := &stubRequester{replies: map[string]*nats.Msg{
		insightsQuerySubject: {Data: []byte(`[{"n":1}]`), Header: nats.Header{}},
	}}
	src := &insightsSource{nc: stub, timeout: time.Second}

	out, isErr := callTool(t, src.Tools(), "insights_query", `{"sql":"SELECT 1"}`)
	is.True(!isErr)
	is.Equal(out, `[{"n":1}]`)
	is.Equal(stub.lastSubject, insightsQuerySubject)
	// The tool input is forwarded verbatim as the request payload.
	var payload map[string]string
	is.NoErr(json.Unmarshal(stub.lastPayload, &payload))
	is.Equal(payload["sql"], "SELECT 1")
}

func TestInsightsSourceEmptyInputSendsEmptyObject(t *testing.T) {
	is := is.New(t)
	stub := &stubRequester{}
	src := &insightsSource{nc: stub, timeout: time.Second}

	_, isErr := callTool(t, src.Tools(), "insights_schemas", ``)
	is.True(!isErr)
	is.Equal(string(stub.lastPayload), `{}`)
}

func TestInsightsSourceMicroError(t *testing.T) {
	is := is.New(t)
	stub := &stubRequester{replies: map[string]*nats.Msg{
		insightsQuerySubject: {Header: nats.Header{
			"Nats-Service-Error":      []string{"sql is required"},
			"Nats-Service-Error-Code": []string{"400"},
		}},
	}}
	src := &insightsSource{nc: stub, timeout: time.Second}

	out, isErr := callTool(t, src.Tools(), "insights_query", `{}`)
	is.True(isErr)
	is.Equal(out, "Insights API error (code 400): sql is required")
}

func TestInsightsSourceHealthCheck(t *testing.T) {
	is := is.New(t)
	src := &insightsSource{nc: &stubRequester{}, timeout: time.Second}
	is.NoErr(src.HealthCheck(context.Background()))

	src = &insightsSource{nc: &stubRequester{err: errors.New("no responders")}, timeout: time.Second}
	is.True(src.HealthCheck(context.Background()) != nil)
}
