package impact

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matryer/is"
)

func TestOpenAIConversation(t *testing.T) {
	is := is.New(t)

	var requests []map[string]any
	responses := []string{
		`{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
			{"id":"call_1","type":"function","function":{"name":"insights_query","arguments":"{\"sql\":\"SELECT 1\"}"}}
		]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":80,"completion_tokens":20}}`,
		`{"choices":[{"message":{"role":"assistant","content":"all done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"completion_tokens":10}}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		is.Equal(r.URL.Path, "/v1/chat/completions")
		is.Equal(r.Header.Get("Authorization"), "Bearer sk-test")
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		is.NoErr(json.Unmarshal(body, &req))
		requests = append(requests, req)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, responses[len(requests)-1])
	}))
	defer srv.Close()

	p := newOpenAIProvider(AgentConfig{
		Provider: "openai-compatible", Model: "some-model", APIKey: "sk-test",
		BaseURL: srv.URL + "/v1", MaxResponseTokens: 4000,
	})
	conv := p.Start("system prompt", []ToolDef{{Name: "insights_query", Description: "q", InputSchema: json.RawMessage(`{"type":"object"}`)}})

	turn, err := conv.Send(context.Background(), "analyze", nil)
	is.NoErr(err)
	is.Equal(len(turn.ToolCalls), 1)
	is.Equal(turn.ToolCalls[0].ID, "call_1")
	is.Equal(string(turn.ToolCalls[0].Input), `{"sql":"SELECT 1"}`)

	turn, err = conv.Send(context.Background(), "", []ToolResult{{ID: "call_1", Content: "[]", IsError: true}})
	is.NoErr(err)
	is.Equal(turn.Text, "all done")
	is.Equal(turn.Usage.InputTokens, 120)

	// History: system, user, assistant (raw), tool.
	msgs := requests[1]["messages"].([]any)
	is.Equal(len(msgs), 4)
	is.Equal(msgs[0].(map[string]any)["role"], "system")
	is.Equal(msgs[2].(map[string]any)["role"], "assistant")
	tool := msgs[3].(map[string]any)
	is.Equal(tool["role"], "tool")
	is.Equal(tool["tool_call_id"], "call_1")
	// Error results are prefixed since the tool role has no is_error field.
	is.Equal(tool["content"], "ERROR: []")

	// Tool defs use the function wrapper.
	tools := requests[0]["tools"].([]any)
	is.Equal(tools[0].(map[string]any)["type"], "function")
}
