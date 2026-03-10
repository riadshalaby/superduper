#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TASKS_FILE="$REPO_ROOT/.ai/TASKS.md"
HANDOFF_FILE="$REPO_ROOT/.ai/HANDOFF.md"
HANDOFF_TEMPLATE_FILE="$REPO_ROOT/.ai/HANDOFF.template.md"

usage() {
  cat <<'EOF'
Usage:
  scripts/ai-check-review.sh <TASK_ID> [reviewer-agent]

Example:
  scripts/ai-check-review.sh T-042 claude
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

task_id="$1"
reviewer_agent="${2:-claude}"

if [[ ! -f "$TASKS_FILE" ]]; then
  echo "Missing file: $TASKS_FILE" >&2
  exit 1
fi

if [[ ! -f "$HANDOFF_FILE" ]]; then
  if [[ -f "$HANDOFF_TEMPLATE_FILE" ]]; then
    cp "$HANDOFF_TEMPLATE_FILE" "$HANDOFF_FILE"
  else
    echo "Missing files: $HANDOFF_FILE and $HANDOFF_TEMPLATE_FILE" >&2
    exit 1
  fi
fi

status_ok=false
if rg -n "^\|[[:space:]]*${task_id}[[:space:]]*\|.*\|[[:space:]]*ready_for_review[[:space:]]*\|" "$TASKS_FILE" >/dev/null; then
  status_ok=true
fi

if [[ "$status_ok" != true ]]; then
  echo "Task $task_id is not in status ready_for_review in .ai/TASKS.md" >&2
  exit 1
fi

handoff_ok=false
if rg -Uz --pcre2 -- "- Task ID:[[:space:]]*${task_id}(.|\n){0,700}- Role:[[:space:]]*implementer(.|\n){0,700}- Commit:[[:space:]]+.+(.|\n){0,700}- Next role:[[:space:]]*review" "$HANDOFF_FILE" >/dev/null; then
  handoff_ok=true
fi

if [[ "$handoff_ok" != true ]]; then
  echo "Implementer handoff block with commit for task $task_id not found in .ai/HANDOFF.md" >&2
  exit 1
fi

echo "Review gate check PASSED for $task_id."
echo "Next step (manual, not auto-started):"
echo "  scripts/ai-review.sh $reviewer_agent"
echo "Then explicitly tell reviewer to start task $task_id."
