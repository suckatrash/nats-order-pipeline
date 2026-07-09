package impact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// anthropicProvider drives the Claude Messages API with plain HTTP. A
// hand-rolled client keeps the dependency footprint at zero and the provider
// contract honest — the adapter is ~200 lines either way.
type anthropicProvider struct {
	cfg    AgentConfig
	client *http.Client
}

func newAnthropicProvider(cfg AgentConfig) *anthropicProvider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.anthropic.com"
	}
	// No client timeout: individual requests are bounded by the run context,
	// and large-context turns can legitimately take minutes.
	return &anthropicProvider{cfg: cfg, client: &http.Client{}}
}

func (p *anthropicProvider) Name() string { return "anthropic" }

func (p *anthropicProvider) Start(system string, tools []ToolDef) Conversation {
	wire := make([]anthropicTool, len(tools))
	for i, t := range tools {
		wire[i] = anthropicTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema}
	}
	return &anthropicConversation{p: p, system: system, tools: wire}
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// anthropicMessage keeps Content as raw JSON. Assistant turns are echoed back
// verbatim from the response, which preserves thinking blocks (and any block
// types this adapter does not know about) across the round trip — required
// for adaptive thinking to work in multi-turn tool use.
type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicRequest struct {
	Model     string                 `json:"model"`
	MaxTokens int                    `json:"max_tokens"`
	System    []anthropicSystemBlock `json:"system"`
	Thinking  *anthropicThinking     `json:"thinking,omitempty"`
	Tools     []anthropicTool        `json:"tools"`
	Messages  []anthropicMessage     `json:"messages"`
}

type anthropicThinking struct {
	Type string `json:"type"`
}

// anthropicSystemBlock carries the system prompt in block form so a
// cache_control breakpoint can sit on it. Tools render before system, so the
// one breakpoint caches both across the run's turns — the embedded skill is
// large and every turn resends it otherwise.
type anthropicSystemBlock struct {
	Type         string                 `json:"type"`
	Text         string                 `json:"text"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicCacheControl struct {
	Type string `json:"type"`
}

type anthropicResponse struct {
	Content    json.RawMessage `json:"content"`
	StopReason string          `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// anthropicBlock is the probe shape used to read text and tool_use blocks out
// of a response's content array; other block types are ignored here but still
// round-trip in the raw history.
type anthropicBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// userBlock builds the blocks of a user message: initial text and/or tool
// results.
type userBlock struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

type anthropicConversation struct {
	p       *anthropicProvider
	system  string
	tools   []anthropicTool
	history []anthropicMessage
}

func (c *anthropicConversation) Send(ctx context.Context, userText string, results []ToolResult) (*Turn, error) {
	var blocks []userBlock
	// Tool results must come first in the user turn.
	for _, r := range results {
		blocks = append(blocks, userBlock{Type: "tool_result", ToolUseID: r.ID, Content: r.Content, IsError: r.IsError})
	}
	if userText != "" {
		blocks = append(blocks, userBlock{Type: "text", Text: userText})
	}
	content, err := json.Marshal(blocks)
	if err != nil {
		return nil, err
	}
	c.history = append(c.history, anthropicMessage{Role: "user", Content: content})

	req := anthropicRequest{
		Model:     c.p.cfg.Model,
		MaxTokens: c.p.cfg.MaxResponseTokens,
		System: []anthropicSystemBlock{{
			Type:         "text",
			Text:         c.system,
			CacheControl: &anthropicCacheControl{Type: "ephemeral"},
		}},
		Tools:    c.tools,
		Messages: c.history,
	}
	if c.p.cfg.Thinking == "adaptive" {
		req.Thinking = &anthropicThinking{Type: "adaptive"}
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := httpDo(ctx, c.p.client, func() (*http.Request, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.p.cfg.BaseURL+"/v1/messages", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("x-api-key", c.p.cfg.APIKey)
		r.Header.Set("anthropic-version", "2023-06-01")
		return r, nil
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anthropic API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed anthropicResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	// Echo the assistant content verbatim into history before reading it.
	c.history = append(c.history, anthropicMessage{Role: "assistant", Content: parsed.Content})

	var probe []anthropicBlock
	if err := json.Unmarshal(parsed.Content, &probe); err != nil {
		return nil, fmt.Errorf("unmarshal content blocks: %w", err)
	}
	turn := &Turn{
		StopReason: parsed.StopReason,
		Usage:      TokenUsage{InputTokens: parsed.Usage.InputTokens, OutputTokens: parsed.Usage.OutputTokens},
	}
	var texts []string
	for _, b := range probe {
		switch b.Type {
		case "text":
			texts = append(texts, b.Text)
		case "tool_use":
			turn.ToolCalls = append(turn.ToolCalls, ToolCall{ID: b.ID, Name: b.Name, Input: b.Input})
		}
	}
	turn.Text = strings.Join(texts, "\n")
	return turn, nil
}
