// Package impact implements the one-shot impact-analysis agent behind the
// impact CLI. It interprets a unified diff against live operational data
// sources (Insights over NATS first), optionally inspects a local repository
// clone for code and history context, and produces an evidence-gated report.
// See impact/SKILL.md for the analysis methodology the agent follows.
package impact

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so config values parse from YAML strings like
// "5m" (yaml.v3 has no native duration support).
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config is the impact.yaml file. Every field has a working default except
// credentials and the Insights endpoint.
type Config struct {
	Agent       AgentConfig       `yaml:"agent"`
	Findings    FindingsConfig    `yaml:"findings"`
	Datasources DatasourcesConfig `yaml:"datasources"`
}

// AgentConfig selects and tunes the model provider.
type AgentConfig struct {
	// Provider is one of "anthropic", "openai", or "openai-compatible".
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
	APIKey   string `yaml:"api_key"`
	// BaseURL overrides the provider endpoint. Required for
	// openai-compatible (e.g. a local model server); optional for the named
	// providers, which default to their public endpoints.
	BaseURL string   `yaml:"base_url"`
	Timeout Duration `yaml:"timeout"`
	// MaxIterations caps the number of model round-trips per run.
	MaxIterations int `yaml:"max_iterations"`
	// TokenBudget is the hard per-run ceiling on input+output tokens. When
	// exceeded the run stops and the report is marked truncated.
	TokenBudget int `yaml:"token_budget"`
	// MaxResponseTokens is the per-response output cap sent to the provider.
	MaxResponseTokens int `yaml:"max_response_tokens"`
	// Thinking, when "adaptive", enables adaptive thinking on the anthropic
	// provider (recommended for Claude 4.6+ models; leave empty for models
	// that do not accept the parameter). Ignored by other providers.
	Thinking string `yaml:"thinking"`
}

// FindingsConfig tunes the evidence gates. Both values are injected into the
// agent's instructions; they are policy, not code paths.
type FindingsConfig struct {
	// StalenessBound suppresses findings whose evidence is older than this.
	StalenessBound Duration `yaml:"staleness_bound"`
	// HeadroomHorizon is the lookahead window for HEADROOM_EXHAUSTION.
	HeadroomHorizon Duration `yaml:"headroom_horizon"`
}

// DatasourcesConfig lists the configured data sources. At least one is
// required; insights is the only implementation today.
type DatasourcesConfig struct {
	Insights *InsightsConfig `yaml:"insights"`
}

// InsightsConfig connects to the Insights query API over NATS.
type InsightsConfig struct {
	Endpoint string `yaml:"endpoint"`
	Creds    string `yaml:"creds"`
}

// DefaultConfig returns the documented defaults; LoadConfig layers the file
// on top of it.
func DefaultConfig() *Config {
	return &Config{
		Agent: AgentConfig{
			Provider:          "anthropic",
			Model:             "claude-opus-4-8",
			Timeout:           Duration(5 * time.Minute),
			MaxIterations:     50,
			TokenBudget:       500_000,
			MaxResponseTokens: 16_000,
			Thinking:          "adaptive",
		},
		Findings: FindingsConfig{
			StalenessBound:  Duration(10 * time.Minute),
			HeadroomHorizon: Duration(24 * time.Hour),
		},
	}
}

// envRef matches ${VAR} only — deliberately not bare $VAR, so YAML content
// containing a dollar sign (SQL, regexes) survives expansion untouched.
var envRef = regexp.MustCompile(`\$\{(\w+)\}`)

func expandEnv(b []byte) []byte {
	return envRef.ReplaceAllFunc(b, func(m []byte) []byte {
		name := envRef.FindSubmatch(m)[1]
		return []byte(os.Getenv(string(name)))
	})
}

// LoadConfig reads path over the defaults. An empty path returns the
// defaults unchanged; a named file must exist.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(expandEnv(b)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		// An empty (or comment-only) config file means "all defaults".
		if errors.Is(err, io.EOF) {
			return cfg, nil
		}
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks the invariants the run depends on. It is called after flag
// overrides are applied.
func (c *Config) Validate() error {
	switch c.Agent.Provider {
	case "anthropic", "openai":
		if c.Agent.APIKey == "" {
			return fmt.Errorf("agent.api_key is required for provider %q", c.Agent.Provider)
		}
	case "openai-compatible":
		// Local model servers commonly need no key, but always need a URL.
		if c.Agent.BaseURL == "" {
			return fmt.Errorf("agent.base_url is required for provider %q", c.Agent.Provider)
		}
	default:
		return fmt.Errorf("unknown agent.provider %q (anthropic, openai, openai-compatible)", c.Agent.Provider)
	}
	if c.Agent.Model == "" {
		return fmt.Errorf("agent.model is required")
	}
	if c.Agent.MaxIterations <= 0 {
		return fmt.Errorf("agent.max_iterations must be positive")
	}
	if c.Agent.MaxResponseTokens <= 0 {
		return fmt.Errorf("agent.max_response_tokens must be positive")
	}
	// 0 disables the budget; negative values are misconfiguration.
	if c.Agent.TokenBudget < 0 {
		return fmt.Errorf("agent.token_budget must be >= 0 (0 disables the budget)")
	}
	if c.Datasources.Insights == nil {
		return fmt.Errorf("at least one data source is required (datasources.insights)")
	}
	if c.Datasources.Insights.Endpoint == "" {
		return fmt.Errorf("datasources.insights.endpoint is required")
	}
	return nil
}
