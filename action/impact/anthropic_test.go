package impact

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestAnthropicConversation(t *testing.T) {
	is := is.New(t)

	var requests []map[string]any
	responses := []string{
		// Turn 1: thinking + text + tool_use. The unknown-to-the-probe
		// thinking block must survive the history round trip.
		`{"content":[
			{"type":"thinking","thinking":"let me check","signature":"sig1"},
			{"type":"text","text":"querying"},
			{"type":"tool_use","id":"tu_1","name":"insights_query","input":{"sql":"SELECT 1"}}
		],"stop_reason":"tool_use","usage":{"input_tokens":100,"output_tokens":50}}`,
		// Turn 2: done.
		`{"content":[{"type":"text","text":"all done"}],"stop_reason":"end_turn","usage":{"input_tokens":200,"output_tokens":25}}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		is.Equal(r.Header.Get("x-api-key"), "sk-test")
		is.Equal(r.Header.Get("anthropic-version"), "2023-06-01")
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		is.NoErr(json.Unmarshal(body, &req))
		requests = append(requests, req)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, responses[len(requests)-1])
	}))
	defer srv.Close()

	p := newAnthropicProvider(AgentConfig{
		Provider: "anthropic", Model: "claude-opus-4-8", APIKey: "sk-test",
		BaseURL: srv.URL, MaxResponseTokens: 16000, Thinking: "adaptive",
	})
	conv := p.Start("system prompt", []ToolDef{{Name: "insights_query", Description: "q", InputSchema: json.RawMessage(`{"type":"object"}`)}})

	turn, err := conv.Send(context.Background(), "analyze this", nil)
	is.NoErr(err)
	is.Equal(turn.Text, "querying")
	is.Equal(len(turn.ToolCalls), 1)
	is.Equal(turn.ToolCalls[0].Name, "insights_query")
	is.Equal(turn.Usage.InputTokens, 100)

	turn, err = conv.Send(context.Background(), "", []ToolResult{{ID: "tu_1", Content: `[{"n":1}]`}})
	is.NoErr(err)
	is.Equal(turn.Text, "all done")
	is.Equal(len(turn.ToolCalls), 0)
	is.Equal(turn.StopReason, "end_turn")

	// Request 1 carries system (in block form, with a cache breakpoint),
	// tools, thinking, and the initial user turn.
	req1 := requests[0]
	sysBlock := req1["system"].([]any)[0].(map[string]any)
	is.Equal(sysBlock["text"], "system prompt")
	is.Equal(sysBlock["cache_control"].(map[string]any)["type"], "ephemeral")
	is.Equal(req1["model"], "claude-opus-4-8")
	is.Equal(req1["thinking"].(map[string]any)["type"], "adaptive")
	is.Equal(len(req1["tools"].([]any)), 1)

	// Request 2 history: user, assistant (verbatim, thinking included), user
	// with the tool_result first.
	msgs := requests[1]["messages"].([]any)
	is.Equal(len(msgs), 3)
	assistant := msgs[1].(map[string]any)
	is.Equal(assistant["role"], "assistant")
	blocks := assistant["content"].([]any)
	is.Equal(blocks[0].(map[string]any)["type"], "thinking")
	is.Equal(blocks[0].(map[string]any)["signature"], "sig1")
	lastUser := msgs[2].(map[string]any)["content"].([]any)
	is.Equal(lastUser[0].(map[string]any)["type"], "tool_result")
	is.Equal(lastUser[0].(map[string]any)["tool_use_id"], "tu_1")
}

func TestAnthropicAPIError(t *testing.T) {
	is := is.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`)
	}))
	defer srv.Close()

	p := newAnthropicProvider(AgentConfig{Model: "m", APIKey: "k", BaseURL: srv.URL, MaxResponseTokens: 100})
	conv := p.Start("s", nil)
	_, err := conv.Send(context.Background(), "hi", nil)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "400"))
}
