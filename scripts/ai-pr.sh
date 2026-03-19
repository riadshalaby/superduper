#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage:
  scripts/ai-pr.sh sync [--base <branch>] [--title <title>] [--dry-run]

Examples:
  scripts/ai-pr.sh sync
  scripts/ai-pr.sh sync --title "feat: add retry metrics"
  scripts/ai-pr.sh sync --dry-run
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

determine_pr_base_ref() {
  local base_branch="$1"
  if git rev-parse --verify --quiet "refs/remotes/origin/$base_branch" >/dev/null; then
    printf 'origin/%s' "$base_branch"
  elif git rev-parse --verify --quiet "refs/heads/$base_branch" >/dev/null; then
    printf '%s' "$base_branch"
  else
    die "Cannot determine PR base ref (expected origin/$base_branch or $base_branch)."
  fi
}

build_commit_list_markdown() {
  local range="$1"
  local commits
  commits="$(git log --reverse --no-merges --format='- %h %s' "$range")"
  if [[ -z "$commits" ]]; then
    echo "- no commits detected in range"
  else
    echo "$commits"
  fi
}

build_breaking_changes_markdown() {
  local range="$1"
  git log --reverse --no-merges --format='%s' "$range" | awk '
{
  subject = $0
  if (subject !~ /^[a-z]+(\([^)]+\))?!: /) {
    next
  }
  note = subject
  sub(/^[a-z]+(\([^)]+\))?!: /, "", note)
  sub(/[[:space:]]+$/, "", note)
  if (note == "" || seen[note]++) {
    next
  }
  printf "- %s\n", note
  total++
}
END {
  if (total == 0) {
    print "<!-- None. Remove this section if not needed. -->"
  }
}
'
}

find_existing_pr_number() {
  local branch="$1"
  local base_branch="$2"
  gh pr list --head "$branch" --base "$base_branch" --state open --limit 1 --json number --jq '.[0].number // empty'
}

existing_or_default_title() {
  local existing_pr_number="$1"
  local explicit_title="${2:-}"
  if [[ -n "$explicit_title" ]]; then
    printf '%s' "$explicit_title"
  elif [[ -n "$existing_pr_number" ]]; then
    gh pr view "$existing_pr_number" --json title --jq '.title'
  else
    git log -1 --format='%s'
  fi
}

sync_pr() {
  local base_branch="main"
  local explicit_title=""
  local dry_run="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --base)
        shift
        [[ $# -gt 0 ]] || die "--base requires a value"
        base_branch="$1"
        ;;
      --title)
        shift
        [[ $# -gt 0 ]] || die "--title requires a value"
        explicit_title="$1"
        ;;
      --dry-run)
        dry_run="true"
        ;;
      *)
        die "Unknown option for sync: $1"
        ;;
    esac
    shift
  done

  require_cmd git
  if [[ "$dry_run" != "true" ]]; then
    require_cmd gh
  fi

  local branch
  branch="$(git rev-parse --abbrev-ref HEAD)"
  [[ "$branch" != "$base_branch" ]] || die "Run sync on a feature branch, not $base_branch."

  if git remote get-url origin >/dev/null 2>&1; then
    git fetch origin "$base_branch" >/dev/null 2>&1 || true
  fi

  local base_ref
  base_ref="$(determine_pr_base_ref "$base_branch")"
  local merge_base
  merge_base="$(git merge-base "$base_ref" HEAD)"
  local range
  range="${merge_base}..HEAD"
  local commit_count
  commit_count="$(git rev-list --count "$range")"
  [[ "$commit_count" != "0" ]] || die "No commits detected between $base_ref and HEAD."

  local existing_pr_number=""
  if [[ "$dry_run" != "true" ]]; then
    existing_pr_number="$(find_existing_pr_number "$branch" "$base_branch")"
  fi

  local pr_title
  pr_title="$(existing_or_default_title "$existing_pr_number" "$explicit_title")"
  local commit_list
  commit_list="$(build_commit_list_markdown "$range")"
  local breaking_changes
  breaking_changes="$(build_breaking_changes_markdown "$range")"

  local pr_body
  pr_body="$(cat <<EOF
## Summary

- source branch: $branch
- base branch: $base_branch
- commits in PR: $commit_count

## Breaking Changes

$breaking_changes

## Included Commits

$commit_list

## Test Plan

- [ ] Tests pass locally (\`mvn -T 1C -q test\`)
- [ ] Formatting passes (\`mvn -q spotless:check\`)
EOF
)"

  if [[ "$dry_run" == "true" ]]; then
    printf 'Title: %s\n\n%s\n' "$pr_title" "$pr_body"
    return 0
  fi

  git push -u origin "$branch"

  if [[ -n "$existing_pr_number" ]]; then
    gh pr edit "$existing_pr_number" --title "$pr_title" --body "$pr_body"
    echo "Updated existing PR #$existing_pr_number for branch $branch."
  else
    gh pr create --base "$base_branch" --head "$branch" --title "$pr_title" --body "$pr_body"
  fi
}

main() {
  [[ $# -gt 0 ]] || {
    usage
    exit 1
  }

  case "$1" in
    sync)
      shift
      sync_pr "$@"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      die "Unknown command: $1"
      ;;
  esac
}

main "$@"
