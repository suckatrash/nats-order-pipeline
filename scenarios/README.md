# Impact-analysis scenario evals

Each directory is one scenario: a frozen `scenario.diff` fixture (never
applied, never committed to a branch) plus an `expect.json` stating
which finding codes **must** fire, which **must not** (bait), and
optionally the exact expected risk level. `run.sh` builds the CLI from
`action/`, runs every scenario against the live lab Insights, and
asserts on the JSON output — codes and risk only, never prose.

```sh
export ANTHROPIC_API_KEY=... INSIGHTS_NATS_SERVER=... INSIGHTS_NATS_CREDS=...
scenarios/run.sh                      # full suite
scenarios/run.sh replica-drop-noop    # one scenario
RUNS=3 scenarios/run.sh               # consistency check: codes must be stable
```

## Lab-state assumptions

Scenarios assert against **live** data, so each `expect.json` carries
an `assumes` line describing the lab state it needs (e.g. "ORDERS holds
well over 256MiB"). If the lab drifts — stream purged, consumer paused
long enough to build lag — re-seed the state or expect failures that
are the lab's fault, not the tool's. Preconditions are documentation
for now; automating them (query-then-skip) is future work.

The bait scenarios (`must_not`) matter as much as the trigger ones:
the tool's stated goal is a low false-positive rate, and every bait
that fires is a catalog rule or skill instruction that needs
tightening.

`last-report.json` / `last-run.log` are droppings from the most recent
run, gitignored, kept for debugging.
