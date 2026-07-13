# NATS Impact Analysis action

Composite GitHub Action that runs a one-shot, evidence-gated impact
analysis of a diff against live NATS operational data (Insights over
NATS), then writes a markdown report for the caller to publish.

## Source layout

The `impact` CLI source is **vendored** here (`impact/`, `cmd/impact/`)
so the action is self-contained and builds at run time — no binaries
are committed. This copy was taken from the
`github.com/ConnectEverything/insights` repo (`impact/` and
`cmd/impact/`) at commit `f8b2782` and has since diverged: HTTP retry
hardening (`provider.go`), the prometheus datasource
(`prometheus_source.go`), and the natsdocs knowledge source
(`natsdocs_source.go`) exist only here. Treat this copy as the
working source until the tool gets its own repo.

## Knowledge corpus

`impact/knowledge/` vendors the [nats-op-costs](https://github.com/synadia-labs/nats-op-costs)
corpus — per-operation cost classifications the `natsdocs` source embeds
and serves to the agent. It is generated, not hand-edited; refresh it
from a local checkout with:

```sh
./refresh-knowledge.sh [path-to-nats-op-costs]   # default ../../nats-op-costs
```

The source is on by default (it needs no configuration); disable it with
`datasources.natsdocs.enabled: false` in `impact.yaml`.

To re-vendor after upstream changes:

```sh
cp $INSIGHTS/impact/*.go $INSIGHTS/impact/SKILL.md action/impact/
cp $INSIGHTS/cmd/impact/main.go action/cmd/impact/
# rewrite the module-internal import path
sed -i '' 's#github.com/ConnectEverything/insights/impact#github.com/suckatrash/nats-order-pipeline/action/impact#' action/cmd/impact/main.go
cd action && go mod tidy && go test ./...
```

## Inputs

See `action.yml`. Configuration beyond the inputs (model, token
budget, finding thresholds) comes from `impact.yaml` in the caller's
workspace; `${INSIGHTS_NATS_SERVER}` / `${INSIGHTS_NATS_CREDS}` /
`${PROMETHEUS_PASSWORD}` references in it expand from the env this
action sets.

## Publishing

The action never talks to GitHub. The calling workflow owns
publishing, e.g. `gh pr comment --body-file <output_file>`.
