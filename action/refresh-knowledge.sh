#!/usr/bin/env bash
# Refresh the vendored nats-op-costs knowledge corpus in impact/knowledge/
# from a local checkout of https://github.com/synadia-labs/nats-op-costs.
#
# Usage: ./refresh-knowledge.sh [path-to-nats-op-costs-checkout]
#
# Once the corpus artifacts are published (nats-op-costs.vercel.app), this can
# switch to fetching /operations.json and /ops/*.md instead of building
# locally. Review the resulting diff like any other change.
set -euo pipefail

cd "$(dirname "$0")"
src="${1:-../../nats-op-costs}"

if [ ! -f "$src/data/operations.json" ]; then
  echo "error: $src does not look like a nats-op-costs checkout" >&2
  exit 1
fi

make -C "$src" llms

rm -rf impact/knowledge
mkdir -p impact/knowledge/ops
cp "$src/dist/operations.json" impact/knowledge/operations.json
cp "$src"/dist/ops/*.md impact/knowledge/ops/

echo "refreshed impact/knowledge from $src ($(ls impact/knowledge/ops | wc -l | tr -d ' ') operations)"
