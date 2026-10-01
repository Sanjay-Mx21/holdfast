#!/usr/bin/env bash
# Checks db/migrations/<schema>/*.sql:
#   - names follow NNNNN_snake_case.sql and versions are unique per schema;
#   - every file has a "-- +goose Up" section;
#   - migrations that exist on origin/main were not edited or deleted
#     (applied migrations are immutable; add a new one instead).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
status=0

for dir in "$root"/db/migrations/*/; do
  schema="$(basename "$dir")"
  declare -A seen=()
  for f in "$dir"*.sql; do
    [[ -e "$f" ]] || continue
    name="$(basename "$f")"
    if [[ ! "$name" =~ ^([0-9]{5})_[a-z0-9_]+\.sql$ ]]; then
      echo "x $schema/$name: must be named NNNNN_snake_case.sql"; status=1; continue
    fi
    version="${BASH_REMATCH[1]}"
    if [[ -n "${seen[$version]:-}" ]]; then
      echo "x $schema: version $version used by both ${seen[$version]} and $name"; status=1
    fi
    seen[$version]="$name"
    grep -q '^-- +goose Up' "$f" || { echo "x $schema/$name: missing '-- +goose Up'"; status=1; }
  done
  unset seen
done

if git -C "$root" rev-parse --verify --quiet origin/main >/dev/null; then
  changed="$(git -C "$root" diff --name-only --diff-filter=MD origin/main -- db/migrations || true)"
  if [[ -n "$changed" ]]; then
    echo "x existing migrations were modified or deleted (add a new migration instead):"
    echo "$changed"; status=1
  fi
fi

[[ $status -eq 0 ]] && echo "ok: migrations are well-formed"
exit $status
