// action implements a Claude API tool-use loop for impact analysis.
// It queries an Insights instance over NATS and posts the analysis as
// a PR comment via gh CLI.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	nc, err := connectNATS(cfg)
	if err != nil {
		slog.Error("failed to connect to nats", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	diff, err := os.ReadFile(cfg.DiffFile)
	if err != nil {
		slog.Error("failed to read diff", "error", err, "path", cfg.DiffFile)
		os.Exit(1)
	}

	skill, err := os.ReadFile(cfg.SkillFile)
	if err != nil {
		slog.Error("failed to read skill", "error", err, "path", cfg.SkillFile)
		os.Exit(1)
	}

	analysis, err := runToolLoop(ctx, cfg, nc, string(skill), string(diff))
	if err != nil {
		slog.Error("tool-use loop failed", "error", err)
		os.Exit(1)
	}

	if err := os.WriteFile(cfg.OutputFile, []byte(analysis), 0o644); err != nil {
		slog.Error("failed to write output", "error", err)
		os.Exit(1)
	}

	slog.Info("analysis complete", "output", cfg.OutputFile, "length", len(analysis))
}

// config holds all action configuration sourced from environment/inputs.
type config struct {
	DiffFile      string
	SkillFile     string
	OutputFile    string
	NATSServer    string
	NATSCreds     string
	AnthropicKey  string
	Model         string
	MaxTurns      int
	RequestTimeout time.Duration
}

func loadConfig() (*config, error) {
	cfg := &config{
		DiffFile:       envOr("INPUT_DIFF_FILE", "/tmp/change.diff"),
		SkillFile:      envOr("INPUT_SKILL_FILE", ".claude/skills/impact-analysis/SKILL.md"),
		OutputFile:     envOr("INPUT_OUTPUT_FILE", "/tmp/analysis-output.md"),
		NATSServer:     os.Getenv("INPUT_NATS_SERVER"),
		NATSCreds:      envOr("INPUT_NATS_CREDS", ""),
		AnthropicKey:   os.Getenv("INPUT_ANTHROPIC_API_KEY"),
		Model:          envOr("INPUT_MODEL", "claude-sonnet-4-20250514"),
		MaxTurns:       20,
		RequestTimeout: 30 * time.Second,
	}

	if cfg.NATSServer == "" {
		return nil, fmt.Errorf("INPUT_NATS_SERVER is required")
	}
	if cfg.AnthropicKey == "" {
		return nil, fmt.Errorf("INPUT_ANTHROPIC_API_KEY is required")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func connectNATS(cfg *config) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("impact-analysis-action"),
		nats.Timeout(10 * time.Second),
	}
	if cfg.NATSCreds != "" {
		opts = append(opts, nats.UserCredentials(cfg.NATSCreds))
	}
	return nats.Connect(cfg.NATSServer, opts...)
}

// --- Claude API types ---

type messagesRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	System    string          `json:"system"`
	Tools     []toolDef       `json:"tools"`
	Messages  []message       `json:"messages"`
}

type message struct {
	Role    string        `json:"role"`
	Content []contentBlock `json:"content"`
}

type contentBlock struct {
	Type  string `json:"type"`
	Text  string `json:"text,omitempty"`
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`

	// tool_result fields
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitzero"`
	IsError   bool   `json:"is_error,omitempty"`
}

type toolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type messagesResponse struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Role       string         `json:"role"`
	Content    []contentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// --- Tool definitions ---

func toolDefinitions() []toolDef {
	return []toolDef{
		{
			Name:        "query",
			Description: "Execute a read-only DuckDB SQL query against the Insights database. Returns JSON array of row objects.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"sql":{"type":"string","description":"SQL query to execute"}},"required":["sql"]}`),
		},
		{
			Name:        "schemas",
			Description: "List queryable database schemas with object counts and descriptions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
		{
			Name:        "tables",
			Description: "List tables and views in a schema with descriptions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string","description":"Schema name (e.g. hx, audit)"}},"required":["schema"]}`),
		},
		{
			Name:        "columns",
			Description: "List columns of a table with types and descriptions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string","description":"Schema name"},"table":{"type":"string","description":"Table name"}},"required":["schema","table"]}`),
		},
		{
			Name:        "macros",
			Description: "List audit check macros with signatures and descriptions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"schema":{"type":"string","description":"Schema name (e.g. audit)"}},"required":["schema"]}`),
		},
		{
			Name:        "read_file",
			Description: "Read a file from the repository.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Relative path from repository root"}},"required":["path"]}`),
		},
	}
}

// --- Tool execution ---

func executeTool(ctx context.Context, nc *nats.Conn, cfg *config, name string, input json.RawMessage) (string, bool) {
	switch name {
	case "query":
		return executeNATSTool(ctx, nc, cfg, "$INS.db.query", input)
	case "schemas":
		return executeNATSTool(ctx, nc, cfg, "$INS.db.schemas", input)
	case "tables":
		return executeNATSTool(ctx, nc, cfg, "$INS.db.tables", input)
	case "columns":
		return executeNATSTool(ctx, nc, cfg, "$INS.db.columns", input)
	case "macros":
		return executeNATSTool(ctx, nc, cfg, "$INS.db.macros", input)
	case "read_file":
		return executeReadFile(input)
	default:
		return fmt.Sprintf("unknown tool: %s", name), true
	}
}

func executeNATSTool(ctx context.Context, nc *nats.Conn, cfg *config, subject string, input json.RawMessage) (string, bool) {
	payload := input
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	msg, err := nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return fmt.Sprintf("NATS request error: %v", err), true
	}

	// Check for NATS micro error headers.
	if msg.Header.Get("Nats-Service-Error") != "" {
		code := msg.Header.Get("Nats-Service-Error-Code")
		errMsg := msg.Header.Get("Nats-Service-Error")
		return fmt.Sprintf("Insights API error (code %s): %s", code, errMsg), true
	}

	return string(msg.Data), false
}

func executeReadFile(input json.RawMessage) (string, bool) {
	var params struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Sprintf("invalid input: %v", err), true
	}

	// Sanitize: prevent path traversal outside the repo.
	clean := filepath.Clean(params.Path)
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "path must be relative and within the repository", true
	}

	data, err := os.ReadFile(clean)
	if err != nil {
		return fmt.Sprintf("read error: %v", err), true
	}

	// Truncate very large files to avoid token bloat.
	const maxBytes = 50_000
	if len(data) > maxBytes {
		return string(data[:maxBytes]) + "\n... (truncated)", false
	}
	return string(data), false
}

// --- Tool-use loop ---

func runToolLoop(ctx context.Context, cfg *config, nc *nats.Conn, skillContent, diffContent string) (string, error) {
	systemPrompt := skillContent + "\n\n" +
		"You have access to the following tools to query the Insights database:\n" +
		"- query(sql): Execute a read-only SQL query\n" +
		"- schemas(): List database schemas\n" +
		"- tables(schema): List tables in a schema\n" +
		"- columns(schema, table): List columns of a table\n" +
		"- macros(schema): List macros in a schema\n" +
		"- read_file(path): Read a file from the repository\n\n" +
		"Use schema discovery (schemas, tables, columns) before writing queries. " +
		"Always qualify table names with the schema prefix (e.g. hx.stream_ident).\n\n" +
		"Produce a complete impact analysis following the output format in the skill. " +
		"Do NOT ask for confirmation — proceed through all steps autonomously and produce the final assessment."

	messages := []message{
		{
			Role: "user",
			Content: []contentBlock{
				{Type: "text", Text: "Analyze the impact of this change:\n\n```diff\n" + diffContent + "\n```"},
			},
		},
	}

	tools := toolDefinitions()
	var totalInputTokens, totalOutputTokens int

	for turn := range cfg.MaxTurns {
		slog.Info("API call", "turn", turn+1)

		resp, err := callClaudeAPI(ctx, cfg, systemPrompt, tools, messages)
		if err != nil {
			return "", fmt.Errorf("turn %d: %w", turn+1, err)
		}

		totalInputTokens += resp.Usage.InputTokens
		totalOutputTokens += resp.Usage.OutputTokens

		// Append assistant response.
		messages = append(messages, message{Role: "assistant", Content: resp.Content})

		// If stop_reason is end_turn, extract final text.
		if resp.StopReason == "end_turn" {
			slog.Info("analysis complete",
				"turns", turn+1,
				"input_tokens", totalInputTokens,
				"output_tokens", totalOutputTokens,
			)
			return extractText(resp.Content), nil
		}

		// Process tool_use blocks.
		if resp.StopReason == "tool_use" {
			var toolResults []contentBlock
			for _, block := range resp.Content {
				if block.Type != "tool_use" {
					continue
				}
				slog.Info("executing tool", "name", block.Name, "id", block.ID)
				result, isError := executeTool(ctx, nc, cfg, block.Name, block.Input)

				// Truncate very long results.
				const maxResult = 100_000
				if len(result) > maxResult {
					result = result[:maxResult] + "\n... (truncated)"
				}

				toolResults = append(toolResults, contentBlock{
					Type:      "tool_result",
					ToolUseID: block.ID,
					Content:   result,
					IsError:   isError,
				})
			}
			messages = append(messages, message{Role: "user", Content: toolResults})
		}
	}

	return "", fmt.Errorf("exceeded max turns (%d)", cfg.MaxTurns)
}

func extractText(blocks []contentBlock) string {
	var parts []string
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// --- Claude Messages API client ---

const anthropicAPIURL = "https://api.anthropic.com/v1/messages"

func callClaudeAPI(ctx context.Context, cfg *config, system string, tools []toolDef, messages []message) (*messagesResponse, error) {
	reqBody := messagesRequest{
		Model:     cfg.Model,
		MaxTokens: 8192,
		System:    system,
		Tools:     tools,
		Messages:  messages,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", anthropicAPIURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", cfg.AnthropicKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	var result messagesResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}
