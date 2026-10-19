#!/usr/bin/env bash
# Refresh contract/ from a checkout of the data model: the two schemas and the generators' manifests (operators/ there).
#   scripts/sync_contract.sh [../data-model]
set -euo pipefail
MODEL="${1:-${DATAMODEL_PATH:-$(dirname "$0")/../../data-model}}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$MODEL/operators/operator.schema.json" ] || { echo "no operators/ contract in $MODEL" >&2; exit 1; }
rm -rf "$HERE/contract"; mkdir -p "$HERE/contract"
cp "$MODEL"/operators/*.schema.json "$HERE/contract/"
for d in "$MODEL"/operators/*/; do [ -f "$d/operator.yaml" ] && { mkdir -p "$HERE/contract/$(basename "$d")"; cp "$d/operator.yaml" "$HERE/contract/$(basename "$d")/"; }; done
git -C "$MODEL" rev-parse HEAD > "$HERE/contract/SOURCE" 2>/dev/null || echo unknown > "$HERE/contract/SOURCE"
echo "contract/ refreshed from $MODEL ($(cat "$HERE/contract/SOURCE"))"
