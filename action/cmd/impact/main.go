// Command impact is the one-shot NATS impact-analysis CLI: it reads a
// unified diff (and optionally a local repository clone), runs an agent
// against the configured operational data sources, and writes an
// evidence-gated report to stdout or a file.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/suckatrash/nats-order-pipeline/action/impact"
)

// errFailOn signals the --fail-on threshold was met; main maps it to exit 2.
var errFailOn = errors.New("risk threshold met")

type cli struct {
	Config  string     `short:"c" help:"Config file path (default: ./impact.yaml when present)." type:"path"`
	Analyze analyzeCmd `cmd:"" default:"withargs" help:"Analyze the impact of a diff against live operational data."`
}

type analyzeCmd struct {
	Diff    string        `help:"Unified diff to analyze; '-' reads stdin." default:"-"`
	Repo    string        `help:"Path to a local clone of the repository the diff applies to." type:"existingdir"`
	Format  string        `help:"Report format." enum:"markdown,json" default:"markdown"`
	Output  string        `short:"o" help:"Write the report to a file instead of stdout." type:"path"`
	FailOn  string        `help:"Exit with code 2 if the risk level meets the threshold." enum:",low,medium,high,critical" default:""`
	Timeout time.Duration `help:"Override the configured max analysis duration."`
	Model   string        `help:"Override the configured model for this run."`
}

func main() {
	var c cli
	kctx := kong.Parse(&c,
		kong.Name("impact"),
		kong.Description("One-shot impact analysis for NATS infrastructure changes."),
		kong.DefaultEnvars("IMPACT"),
		kong.UsageOnError(),
	)
	err := kctx.Run(&c)
	switch {
	case err == nil:
	case errors.Is(err, errFailOn):
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (a *analyzeCmd) Run(c *cli) error {
	// Logs go to stderr; the report owns stdout.
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfgPath := c.Config
	if cfgPath == "" {
		if _, err := os.Stat("impact.yaml"); err == nil {
			cfgPath = "impact.yaml"
		}
	}
	cfg, err := impact.LoadConfig(cfgPath)
	if err != nil {
		return err
	}
	if a.Timeout > 0 {
		cfg.Agent.Timeout = impact.Duration(a.Timeout)
	}
	if a.Model != "" {
		cfg.Agent.Model = a.Model
	}
	// The Anthropic/OpenAI conventions for key material are the natural
	// fallback when the config file does not carry a key.
	if cfg.Agent.APIKey == "" {
		switch cfg.Agent.Provider {
		case "anthropic":
			cfg.Agent.APIKey = os.Getenv("ANTHROPIC_API_KEY")
		case "openai", "openai-compatible":
			cfg.Agent.APIKey = os.Getenv("OPENAI_API_KEY")
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	diff, err := readDiff(a.Diff)
	if err != nil {
		return err
	}
	if strings.TrimSpace(diff) == "" {
		return fmt.Errorf("diff is empty")
	}

	provider, err := impact.NewProvider(cfg.Agent)
	if err != nil {
		return err
	}

	source, closeSource, err := impact.ConnectInsights(cfg.Datasources.Insights)
	if err != nil {
		return err
	}
	defer closeSource()
	sources := []impact.DataSource{source}
	if cfg.Datasources.Prometheus != nil {
		prom, err := impact.ConnectPrometheus(cfg.Datasources.Prometheus)
		if err != nil {
			return err
		}
		sources = append(sources, prom)
	}

	var repo *impact.RepoTools
	if a.Repo != "" {
		repo, err = impact.NewRepoTools(a.Repo)
		if err != nil {
			return err
		}
	}

	// SIGTERM matters for CI/container runners; both signals salvage a
	// partial report rather than killing the run outright.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	analyzer := impact.NewAnalyzer(cfg, provider, sources, repo, log)
	report, err := analyzer.Run(ctx, diff)
	if err != nil {
		return err
	}

	var out []byte
	switch a.Format {
	case "json":
		out, err = report.RenderJSON()
		if err != nil {
			return err
		}
		out = append(out, '\n')
	default:
		out = []byte(report.RenderMarkdown())
	}
	if a.Output != "" {
		if err := os.WriteFile(a.Output, out, 0o644); err != nil {
			return err
		}
	} else {
		if _, err := os.Stdout.Write(out); err != nil {
			return err
		}
	}

	if a.FailOn != "" && impact.RiskAtLeast(report.RiskLevel, a.FailOn) {
		return fmt.Errorf("%w: risk level %s >= %s", errFailOn, report.RiskLevel, a.FailOn)
	}
	return nil
}

func readDiff(path string) (string, error) {
	if path == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", fmt.Errorf("read diff from stdin: %w", err)
		}
		return string(b), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read diff: %w", err)
	}
	return string(b), nil
}
