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
  scripts/ai-check-plan.sh <TASK_ID> [implementer-agent]

Example:
  scripts/ai-check-plan.sh T-042 codex
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

task_id="$1"
implementer_agent="${2:-codex}"

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
if rg -n "^\|[[:space:]]*${task_id}[[:space:]]*\|.*\|[[:space:]]*ready_for_implement[[:space:]]*\|" "$TASKS_FILE" >/dev/null; then
  status_ok=true
fi

if [[ "$status_ok" != true ]]; then
  echo "Task $task_id is not in status ready_for_implement in .ai/TASKS.md" >&2
  exit 1
fi

handoff_ok=false
if awk -v task="$task_id" '
BEGIN { found = 0 }
{
  has_task = ($0 ~ ("- Task ID:[[:space:]]*" task "([[:space:]]|$)"))
  has_role = ($0 ~ "- Role:[[:space:]]*planner([[:space:]]|$)")
  has_next = ($0 ~ "- Next role:[[:space:]]*implement([[:space:]]|$)")

  if (has_task && has_role && has_next) {
    found = 1
    exit 0
  }
}
END { exit found ? 0 : 1 }
' RS='' "$HANDOFF_FILE" >/dev/null; then
  handoff_ok=true
fi

if [[ "$handoff_ok" != true ]]; then
  echo "Planner handoff block for task $task_id not found in .ai/HANDOFF.md" >&2
  exit 1
fi

echo "Plan gate check PASSED for $task_id."
echo "Next step (manual, not auto-started):"
echo "  scripts/ai-implement.sh $implementer_agent"
echo "Then explicitly tell implementer to start task $task_id."
