package impact

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestLoadConfigDefaults(t *testing.T) {
	is := is.New(t)
	cfg, err := LoadConfig("")
	is.NoErr(err)
	is.Equal(cfg.Agent.Provider, "anthropic")
	is.Equal(cfg.Agent.Model, "claude-opus-4-8")
	is.Equal(cfg.Agent.Timeout.Std(), 5*time.Minute)
	is.Equal(cfg.Agent.TokenBudget, 500_000)
	is.Equal(cfg.Findings.StalenessBound.Std(), 10*time.Minute)
}

func TestLoadConfigFileAndEnvExpansion(t *testing.T) {
	is := is.New(t)
	t.Setenv("IMPACT_TEST_KEY", "sk-test-123")
	path := filepath.Join(t.TempDir(), "impact.yaml")
	err := os.WriteFile(path, []byte(`
agent:
  api_key: ${IMPACT_TEST_KEY}
  timeout: 90s
  token_budget: 1000
datasources:
  insights:
    endpoint: nats://example:4222
    creds: /tmp/x.creds
`), 0o600)
	is.NoErr(err)

	cfg, err := LoadConfig(path)
	is.NoErr(err)
	is.Equal(cfg.Agent.APIKey, "sk-test-123")
	is.Equal(cfg.Agent.Timeout.Std(), 90*time.Second)
	is.Equal(cfg.Agent.TokenBudget, 1000)
	// Untouched fields keep their defaults.
	is.Equal(cfg.Agent.Model, "claude-opus-4-8")
	is.Equal(cfg.Datasources.Insights.Endpoint, "nats://example:4222")

	is.NoErr(cfg.Validate())
}

func TestExpandEnvOnlyBracedRefs(t *testing.T) {
	is := is.New(t)
	t.Setenv("IMPACT_TEST_VAR", "value")
	// Bare $ (as in SQL or regex content) must survive; ${...} expands;
	// unset vars expand to empty.
	out := expandEnv([]byte(`a: $literal b: ${IMPACT_TEST_VAR} c: ${IMPACT_TEST_UNSET_VAR}`))
	is.Equal(string(out), "a: $literal b: value c: ")
}

func TestConfigValidate(t *testing.T) {
	is := is.New(t)
	cfg := DefaultConfig()
	// No API key, no datasource.
	is.True(cfg.Validate() != nil)

	cfg.Agent.APIKey = "sk-x"
	is.True(cfg.Validate() != nil) // still no datasource

	cfg.Datasources.Insights = &InsightsConfig{Endpoint: "nats://example:4222"}
	is.NoErr(cfg.Validate())

	cfg.Agent.Provider = "openai-compatible"
	is.True(cfg.Validate() != nil) // needs base_url
	cfg.Agent.BaseURL = "http://localhost:11434/v1"
	is.NoErr(cfg.Validate())

	cfg.Agent.Provider = "nope"
	is.True(cfg.Validate() != nil)
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "impact.yaml")
	err := os.WriteFile(path, []byte("agent:\n  modle: typo\n"), 0o600)
	is.NoErr(err)
	_, err = LoadConfig(path)
	is.True(err != nil)
}
