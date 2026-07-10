package impact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ToolDef is the provider-neutral tool declaration. InputSchema is a JSON
// Schema object; adapters translate it to their wire format.
type ToolDef struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// ToolCall is a model request to run one tool.
type ToolCall struct {
	ID    string
	Name  string
	Input json.RawMessage
}

// ToolResult answers one ToolCall.
type ToolResult struct {
	ID      string
	Content string
	IsError bool
}

// TokenUsage is one turn's token spend as reported by the provider.
type TokenUsage struct {
	InputTokens  int
	OutputTokens int
}

// Turn is one assistant response: any text it produced, the tool calls it
// wants executed (empty when the model is done), and the turn's token usage.
type Turn struct {
	Text       string
	ToolCalls  []ToolCall
	StopReason string
	Usage      TokenUsage
}

// Conversation is one model conversation. The adapter owns the
// provider-specific message history encoding — callers never see it.
//
// The first Send carries the initial user text and nil results; subsequent
// Sends carry the results for the previous Turn's ToolCalls (and empty text).
type Conversation interface {
	Send(ctx context.Context, userText string, results []ToolResult) (*Turn, error)
}

// Provider starts conversations against one model API. Adapters are
// deliberately thin: messages and tool definitions in, text and tool calls
// out — no provider-specific features leak into the contract.
type Provider interface {
	Name() string
	Start(system string, tools []ToolDef) Conversation
}

// NewProvider builds the configured provider adapter.
func NewProvider(cfg AgentConfig) (Provider, error) {
	switch cfg.Provider {
	case "anthropic":
		return newAnthropicProvider(cfg), nil
	case "openai":
		cfg.BaseURL = "https://api.openai.com/v1"
		return newOpenAIProvider(cfg), nil
	case "openai-compatible":
		return newOpenAIProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unknown provider %q", cfg.Provider)
	}
}

// httpDo posts a JSON payload and retries transient failures — transport
// errors, 429, and 5xx — with exponential backoff (1s doubling, capped at
// 30s, 6 attempts ≈ 1m of waiting). Capacity blips like 529 Overloaded
// routinely outlast a short backoff, and abandoning a half-finished analysis
// costs far more than waiting out the blip. The context bounds the whole
// exchange, so the run timeout still wins over retries. Non-retryable
// statuses (4xx other than 429) are returned to the caller, which owns
// reading the error body.
func httpDo(ctx context.Context, client *http.Client, req func() (*http.Request, error)) (*http.Response, error) {
	const maxAttempts = 6
	backoff := time.Second
	for attempt := 0; ; attempt++ {
		r, err := req()
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(r)
		var reason string
		switch {
		case err != nil:
			reason = err.Error()
		case resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
			return resp, nil
		default:
			// Preserve the API's own error detail (rate-limit and overload
			// messages) for the final failure.
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			reason = fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if attempt >= maxAttempts-1 {
			return nil, fmt.Errorf("API request failed after %d attempts: %s", attempt+1, reason)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}
