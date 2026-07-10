#!/usr/bin/env bash
# Impact-analysis scenario eval: run each fixture diff through the CLI
# against the live lab Insights and assert on the JSON finding codes.
#
# Required env: ANTHROPIC_API_KEY, INSIGHTS_NATS_SERVER, INSIGHTS_NATS_CREDS
# Optional:     RUNS=N   repeat each scenario N times (consistency check)
# Usage:        scenarios/run.sh [scenario-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${IMPACT_BIN:-$(mktemp -d)/impact}"
RUNS="${RUNS:-1}"
ONLY="${1:-}"

for v in ANTHROPIC_API_KEY INSIGHTS_NATS_SERVER INSIGHTS_NATS_CREDS; do
  [ -n "${!v:-}" ] || { echo "error: $v is not set" >&2; exit 1; }
done
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

echo "building impact CLI..."
(cd "$ROOT/action" && go build -o "$BIN" ./cmd/impact)

pass=0 fail=0
for dir in "$ROOT"/scenarios/*/; do
  name="$(basename "$dir")"
  [ -f "$dir/scenario.diff" ] || continue
  if [ -n "$ONLY" ] && [ "$name" != "$ONLY" ]; then continue; fi
  expect="$dir/expect.json"

  for run in $(seq 1 "$RUNS"); do
    out="$dir/last-report.json"
    log="$dir/last-run.log"
    if ! "$BIN" analyze --repo "$ROOT" --diff "$dir/scenario.diff" \
        --format json -c "$ROOT/impact.yaml" >"$out" 2>"$log"; then
      echo "FAIL $name (run $run): analyze exited non-zero, see $log"
      fail=$((fail + 1)); continue
    fi

    codes="$(jq -r '[.findings[].code] | unique | join(",")' "$out")"
    risk="$(jq -r '.risk_level' "$out")"
    tokens="$(jq -r '.usage.input_tokens + .usage.output_tokens' "$out")"
    ok=1

    while IFS= read -r c; do
      if ! jq -e --arg c "$c" 'any(.findings[]; .code == $c)' "$out" >/dev/null; then
        echo "FAIL $name (run $run): missing required $c (got: ${codes:-none}, risk=$risk)"; ok=0
      fi
    done < <(jq -r '.must[]?' "$expect")

    while IFS= read -r c; do
      if jq -e --arg c "$c" 'any(.findings[]; .code == $c)' "$out" >/dev/null; then
        echo "FAIL $name (run $run): forbidden $c fired (risk=$risk)"; ok=0
      fi
    done < <(jq -r '.must_not[]?' "$expect")

    want_risk="$(jq -r '.risk // empty' "$expect")"
    if [ -n "$want_risk" ] && [ "$risk" != "$want_risk" ]; then
      echo "FAIL $name (run $run): risk $risk, expected $want_risk"; ok=0
    fi

    if [ "$ok" = 1 ]; then
      echo "PASS $name (run $run): codes=${codes:-none} risk=$risk tokens=$tokens"
      pass=$((pass + 1))
    else
      fail=$((fail + 1))
    fi
  done
done

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
