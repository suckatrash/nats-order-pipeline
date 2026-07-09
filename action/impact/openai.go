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

// openAIProvider drives the Chat Completions API. With a configurable base
// URL it covers OpenAI itself and every OpenAI-compatible server (local
// models, most hosted providers).
type openAIProvider struct {
	cfg    AgentConfig
	client *http.Client
}

func newOpenAIProvider(cfg AgentConfig) *openAIProvider {
	return &openAIProvider{cfg: cfg, client: &http.Client{}}
}

func (p *openAIProvider) Name() string {
	if p.cfg.Provider == "openai" {
		return "openai"
	}
	return "openai-compatible"
}

func (p *openAIProvider) Start(system string, tools []ToolDef) Conversation {
	wire := make([]openAITool, len(tools))
	for i, t := range tools {
		wire[i] = openAITool{Type: "function", Function: openAIFunction{
			Name: t.Name, Description: t.Description, Parameters: t.InputSchema,
		}}
	}
	sys, _ := json.Marshal(map[string]string{"role": "system", "content": system})
	return &openAIConversation{p: p, tools: wire, history: []json.RawMessage{sys}}
}

type openAITool struct {
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type openAIRequest struct {
	Model string `json:"model"`
	// Current OpenAI models reject max_tokens in favor of
	// max_completion_tokens; older OpenAI-compatible servers only know
	// max_tokens. The provider selects which field to send.
	MaxTokens           int               `json:"max_tokens,omitempty"`
	MaxCompletionTokens int               `json:"max_completion_tokens,omitempty"`
	Tools               []openAITool      `json:"tools,omitempty"`
	Messages            []json.RawMessage `json:"messages"`
}

type openAIResponse struct {
	Choices []struct {
		// Message is kept raw so the assistant turn (content + tool_calls)
		// can be appended to history byte-for-byte.
		Message      json.RawMessage `json:"message"`
		FinishReason string          `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

type openAIAssistant struct {
	Content   string `json:"content"`
	ToolCalls []struct {
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls"`
}

type openAIConversation struct {
	p       *openAIProvider
	tools   []openAITool
	history []json.RawMessage
}

func (c *openAIConversation) Send(ctx context.Context, userText string, results []ToolResult) (*Turn, error) {
	// Tool results are individual role:tool messages answering the previous
	// assistant turn's tool_calls; they must precede any new user text.
	for _, r := range results {
		content := r.Content
		if r.IsError {
			content = "ERROR: " + content
		}
		m, err := json.Marshal(map[string]string{"role": "tool", "tool_call_id": r.ID, "content": content})
		if err != nil {
			return nil, err
		}
		c.history = append(c.history, m)
	}
	if userText != "" {
		m, err := json.Marshal(map[string]string{"role": "user", "content": userText})
		if err != nil {
			return nil, err
		}
		c.history = append(c.history, m)
	}

	req := openAIRequest{
		Model:    c.p.cfg.Model,
		Tools:    c.tools,
		Messages: c.history,
	}
	if c.p.cfg.Provider == "openai" {
		req.MaxCompletionTokens = c.p.cfg.MaxResponseTokens
	} else {
		req.MaxTokens = c.p.cfg.MaxResponseTokens
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := strings.TrimSuffix(c.p.cfg.BaseURL, "/") + "/chat/completions"
	resp, err := httpDo(ctx, c.p.client, func() (*http.Request, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		if c.p.cfg.APIKey != "" {
			r.Header.Set("Authorization", "Bearer "+c.p.cfg.APIKey)
		}
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
		return nil, fmt.Errorf("%s API returned %d: %s", c.p.Name(), resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed openAIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return nil, fmt.Errorf("%s API returned no choices", c.p.Name())
	}
	choice := parsed.Choices[0]
	c.history = append(c.history, choice.Message)

	var msg openAIAssistant
	if err := json.Unmarshal(choice.Message, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal assistant message: %w", err)
	}
	turn := &Turn{
		Text:       msg.Content,
		StopReason: choice.FinishReason,
		Usage:      TokenUsage{InputTokens: parsed.Usage.PromptTokens, OutputTokens: parsed.Usage.CompletionTokens},
	}
	for _, tc := range msg.ToolCalls {
		turn.ToolCalls = append(turn.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Input: json.RawMessage(tc.Function.Arguments)})
	}
	return turn, nil
}
