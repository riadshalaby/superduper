#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage:
  scripts/ai-start-cycle.sh <branch-name>

Examples:
  scripts/ai-start-cycle.sh feature/new-scope
  scripts/ai-start-cycle.sh fix/retry-ordering
  scripts/ai-start-cycle.sh chore/docs-cleanup
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

validate_branch_name() {
  local branch_name="$1"

  [[ -n "$branch_name" ]] || {
    usage
    die "branch name is required"
  }

  case "$branch_name" in
    feature/*|fix/*|chore/*)
      ;;
    *)
      usage
      die "branch name must start with feature/, fix/, or chore/"
      ;;
  esac

  [[ "$branch_name" != "feature/" && "$branch_name" != "fix/" && "$branch_name" != "chore/" ]] || {
    usage
    die "branch name must include a suffix after the prefix"
  }

  git check-ref-format --branch "$branch_name" >/dev/null 2>&1 || {
    usage
    die "branch name is not a valid git branch name"
  }
}

branch_exists_remotely() {
  local branch_name="$1"

  git ls-remote --exit-code --heads origin "$branch_name" >/dev/null 2>&1
}

main() {
  local branch_name="${1:-}"

  require_cmd git
  validate_branch_name "$branch_name"

  if git rev-parse --verify --quiet "refs/heads/$branch_name" >/dev/null; then
    die "branch '$branch_name' already exists locally"
  fi

  if branch_exists_remotely "$branch_name"; then
    die "branch '$branch_name' already exists on origin"
  fi

  git checkout main >/dev/null 2>&1 || die "failed to checkout main"
  git pull --ff-only origin main >/dev/null 2>&1 || die "failed to fast-forward local main from origin/main"
  git checkout -b "$branch_name" >/dev/null 2>&1 || die "failed to create branch '$branch_name'"

  cp .ai/PLAN.template.md .ai/PLAN.md
  cp .ai/REVIEW.template.md .ai/REVIEW.md
  cp .ai/TASKS.template.md .ai/TASKS.md
  cp ROADMAP.template.md ROADMAP.md
  rm -f .ai/HANDOFF.md

  git add .ai/PLAN.md .ai/REVIEW.md .ai/TASKS.md ROADMAP.md

  if git ls-files --error-unmatch .ai/HANDOFF.md >/dev/null 2>&1; then
    git rm --cached .ai/HANDOFF.md >/dev/null 2>&1 || die "failed to untrack .ai/HANDOFF.md"
  fi

  git commit -m "chore: start cycle $(basename "$branch_name")" >/dev/null 2>&1 || die "failed to commit cycle bootstrap files"
  git push -u origin "$branch_name" >/dev/null 2>&1 || die "failed to push branch '$branch_name' to origin"

  echo "Started new cycle on branch '$branch_name'."
}

main "$@"
