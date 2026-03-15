#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage:
  scripts/ai-release.sh prepare <X.Y.Z>
  scripts/ai-release.sh finalize <X.Y.Z> [NEXT_VERSION] [--branch <branch-name>] [--archive]

Examples:
  scripts/ai-release.sh prepare 0.5.0
  scripts/ai-release.sh finalize 0.5.0
  scripts/ai-release.sh finalize 0.5.0 0.6.0 --branch feature/v0.6.0 --archive
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

normalize_release_version() {
  local raw="$1"
  local normalized="${raw#v}"
  [[ "$normalized" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "Release version must be X.Y.Z (or vX.Y.Z), got: $raw"
  printf '%s' "$normalized"
}

validate_next_version() {
  local version="$1"
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]] || die "Next version is invalid: $version"
}

AUTO_STASHED="false"
AUTO_STASH_LABEL=""
AUTO_STASH_RESTORED="false"

path_in_list() {
  local needle="$1"
  shift
  local candidate
  for candidate in "$@"; do
    [[ "$candidate" == "$needle" ]] && return 0
  done
  return 1
}

auto_stash_worktree() {
  local preserve_paths=("$@")
  local seen_paths=()
  local stash_paths=()
  local path

  while IFS= read -r -d '' path; do
    [[ -n "$path" ]] || continue
    if [[ ${#seen_paths[@]} -gt 0 ]]; then
      path_in_list "$path" "${seen_paths[@]}" && continue
    fi
    seen_paths+=("$path")
    if [[ ${#preserve_paths[@]} -gt 0 ]]; then
      path_in_list "$path" "${preserve_paths[@]}" && continue
    fi
    stash_paths+=("$path")
  done < <({
    git diff --name-only -z
    git diff --cached --name-only -z
    git ls-files --others --exclude-standard -z
  })

  if [[ ${#stash_paths[@]} -eq 0 ]]; then
    return
  fi

  AUTO_STASH_LABEL="ai-release-$(date -u +%Y%m%dT%H%M%SZ)"
  git stash push -u -m "$AUTO_STASH_LABEL" -- "${stash_paths[@]}" >/dev/null
  AUTO_STASHED="true"
  if [[ ${#preserve_paths[@]} -gt 0 ]]; then
    echo "Temporarily stashed unrelated worktree changes."
  else
    echo "Temporarily stashed existing worktree changes."
  fi
}

restore_worktree() {
  if [[ "$AUTO_STASHED" != "true" || "$AUTO_STASH_RESTORED" == "true" ]]; then
    return 0
  fi

  local stash_ref
  stash_ref="$(git stash list --format='%gd %s' | awk -v label="$AUTO_STASH_LABEL" '$0 ~ label { print $1; exit }')"
  AUTO_STASH_RESTORED="true"

  if [[ -z "$stash_ref" ]]; then
    echo "Warning: could not find auto-stashed worktree entry '$AUTO_STASH_LABEL' to restore." >&2
    return 1
  fi

  if git stash pop "$stash_ref" >/dev/null; then
    echo "Restored stashed worktree changes."
    return 0
  fi

  echo "Warning: failed to restore stashed worktree changes from $stash_ref. Resolve manually with 'git stash list' and 'git stash pop $stash_ref'." >&2
  return 1
}

cleanup_release() {
  local rc=$?
  if ! restore_worktree; then
    rc=1
  fi
  exit "$rc"
}

trap cleanup_release EXIT

project_version() {
  mvn -q -DforceStdout help:evaluate -Dexpression=project.version | tail -n1
}

render_template() {
  local src="$1"
  local dst="$2"
  local version="$3"
  if [[ -f "$src" ]]; then
    sed "s|{{VERSION}}|$version|g" "$src" >"$dst"
  else
    die "Template file not found: $src"
  fi
}

default_next_version_from_release() {
  local release="$1"
  local major minor patch
  IFS='.' read -r major minor patch <<<"$release"
  printf '%s.%s.%s-SNAPSHOT' "$major" "$minor" "$((patch + 1))"
}

determine_pr_base_ref() {
  if git rev-parse --verify --quiet "refs/remotes/origin/main" >/dev/null; then
    printf 'origin/main'
  elif git rev-parse --verify --quiet "refs/heads/main" >/dev/null; then
    printf 'main'
  else
    die "Cannot determine PR base ref (expected origin/main or main)."
  fi
}

build_commit_type_breakdown_markdown() {
  local range="$1"
  git log --no-merges --format='%s' "$range" | awk '
BEGIN {
  order_count = 10
  order[1] = "feat"
  order[2] = "fix"
  order[3] = "perf"
  order[4] = "refactor"
  order[5] = "docs"
  order[6] = "test"
  order[7] = "build"
  order[8] = "ci"
  order[9] = "chore"
  order[10] = "revert"
}
{
  subject = $0
  type = "other"
  if (subject ~ /^[a-z]+(\([^)]+\))?(!)?: /) {
    type = subject
    sub(/[^a-z].*/, "", type)
  }
  counts[type]++
  total++
}
END {
  if (total == 0) {
    print "- no commits detected in range"
    exit
  }
  for (i = 1; i <= order_count; i++) {
    t = order[i]
    if (counts[t] > 0) {
      printf "- %s: %d\n", t, counts[t]
    }
  }
  if (counts["other"] > 0) {
    printf "- other: %d\n", counts["other"]
  }
}
'
}

build_commit_list_markdown() {
  local range="$1"
  local commits
  commits="$(git log --no-merges --format='- %h %s' "$range")"
  if [[ -z "$commits" ]]; then
    echo "- no commits detected in range"
  else
    echo "$commits"
  fi
}

find_existing_pr_number() {
  local branch="$1"
  gh pr list --head "$branch" --base main --state open --limit 1 --json number --jq '.[0].number // empty'
}

prepare_release() {
  local release="$1"
  shift
  [[ $# -eq 0 ]] || die "Unknown option for prepare: $1"
  auto_stash_worktree ".ai/REVIEW.md" ".ai/TASKS.md"

  local branch
  branch="$(git rev-parse --abbrev-ref HEAD)"
  [[ "$branch" != "main" ]] || die "Run prepare on a feature branch, not main."

  mvn versions:set -DnewVersion="$release" -DgenerateBackupPoms=false
  mvn -q -DskipTests test-compile
  mvn -T 1C -q test

  git add -A
  local commit_msg="chore(release): v$release"
  if git diff --cached --quiet; then
    local current_version
    current_version="$(project_version)"
    [[ "$current_version" == "$release" ]] || die \
      "No staged changes after release prepare, and project version is $current_version instead of $release."
    echo "No new release changes to commit; continuing with existing branch state for v$release."
  else
    git commit -m "$commit_msg"
  fi

  git push -u origin "$branch"

  # Refresh remote refs when available so the PR body reflects the latest main.
  if git remote get-url origin >/dev/null 2>&1; then
    git fetch origin main >/dev/null 2>&1 || true
  fi

  local base_ref
  base_ref="$(determine_pr_base_ref)"
  local merge_base
  merge_base="$(git merge-base "$base_ref" HEAD)"
  local range
  range="${merge_base}..HEAD"
  local commit_count
  commit_count="$(git rev-list --count "$range")"
  local commit_type_breakdown
  commit_type_breakdown="$(build_commit_type_breakdown_markdown "$range")"
  local commit_list
  commit_list="$(build_commit_list_markdown "$range")"

  local pr_title="chore: release v$release"
  local pr_body
  pr_body="$(cat <<EOF
## Summary
- release v$release
- source branch: \`$branch\`
- base branch: \`main\`
- commits in PR: $commit_count

## Scope by Commit Type
$commit_type_breakdown

## Included Commits
$commit_list

## Validation
- [x] mvn -q -DskipTests test-compile
- [x] mvn -T 1C -q test

## Release Checklist
- [x] version bumped with mvn versions:set
- [ ] user will merge this PR into main
- [ ] release tag v$release will be created from main after merge
EOF
)"

  require_cmd gh
  local existing_pr_number
  existing_pr_number="$(find_existing_pr_number "$branch")"
  if [[ -n "$existing_pr_number" ]]; then
    gh pr edit "$existing_pr_number" --title "$pr_title" --body "$pr_body"
    echo "Updated existing PR #$existing_pr_number for branch $branch."
  else
    gh pr create --base main --head "$branch" --title "$pr_title" --body "$pr_body"
  fi

  cat <<EOF
Release prepare completed for v$release.

Next steps:
1. Ask the user to review and merge the PR on GitHub.
2. After merge confirmation, run:
   scripts/ai-release.sh finalize $release
EOF
}

finalize_release() {
  local release="$1"
  shift

  local next_version=""
  local branch_name=""
  local archive="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --branch)
        shift
        [[ $# -gt 0 ]] || die "--branch requires a value"
        branch_name="$1"
        ;;
      --archive)
        archive="true"
        ;;
      *)
        if [[ -z "$next_version" ]]; then
          next_version="$1"
        else
          die "Unknown option for finalize: $1"
        fi
        ;;
    esac
    shift
  done

  auto_stash_worktree

  git fetch origin --tags
  git checkout main
  git pull --ff-only origin main

  local current_version
  current_version="$(project_version)"
  [[ "$current_version" == "$release" ]] || die "main version is $current_version, expected $release. Ensure PR was merged."

  local tag="v$release"
  if git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
    echo "Tag $tag already exists locally."
  else
    git tag "$tag"
  fi

  if git ls-remote --tags origin "refs/tags/$tag" | grep -q "$tag"; then
    echo "Tag $tag already exists on origin."
  else
    git push origin "$tag"
  fi

  if [[ -z "$next_version" ]]; then
    local suggested
    suggested="$(default_next_version_from_release "$release")"
    if [[ -t 0 ]]; then
      read -r -p "Enter next development version [$suggested]: " next_version
      next_version="${next_version:-$suggested}"
    else
      die "NEXT_VERSION missing and no interactive terminal available."
    fi
  fi

  validate_next_version "$next_version"

  if [[ -z "$branch_name" ]]; then
    branch_name="feature/v$next_version"
  fi

  if git show-ref --verify --quiet "refs/heads/$branch_name"; then
    die "Local branch already exists: $branch_name"
  fi
  if git ls-remote --heads origin "$branch_name" | grep -q "$branch_name"; then
    die "Remote branch already exists: $branch_name"
  fi

  if [[ "$archive" == "true" ]]; then
    local archive_dir=".ai/archive/v$release"
    mkdir -p "$archive_dir"
    [[ -f ".ai/PLAN.md" ]] && cp ".ai/PLAN.md" "$archive_dir/PLAN.md"
    [[ -f ".ai/REVIEW.md" ]] && cp ".ai/REVIEW.md" "$archive_dir/REVIEW.md"
    [[ -f ".ai/TASKS.md" ]] && cp ".ai/TASKS.md" "$archive_dir/TASKS.md"
    [[ -f "ROADMAP.md" ]] && cp "ROADMAP.md" "$archive_dir/ROADMAP.md"
  fi

  git checkout -b "$branch_name"

  mvn versions:set -DnewVersion="$next_version" -DgenerateBackupPoms=false

  render_template ".ai/PLAN.template.md" ".ai/PLAN.md" "$next_version"
  render_template ".ai/REVIEW.template.md" ".ai/REVIEW.md" "$next_version"
  render_template ".ai/TASKS.template.md" ".ai/TASKS.md" "$next_version"
  render_template "ROADMAP.template.md" "ROADMAP.md" "$next_version"

  rm -f ".ai/HANDOFF.md"

  git add -A
  git diff --cached --quiet && die "No staged changes for next-cycle bootstrap."

  local next_commit_msg="chore: start v$next_version"
  git commit -m "$next_commit_msg"
  git push -u origin "$branch_name"

  cat <<EOF
Release finalize completed for v$release.

Created:
- tag: $tag
- branch: $branch_name
- commit: $next_commit_msg

Cycle files were reset from templates:
- .ai/PLAN.md
- .ai/REVIEW.md
- .ai/TASKS.md
- ROADMAP.md
EOF
}

main() {
  require_cmd git
  require_cmd mvn

  [[ $# -ge 2 ]] || {
    usage
    exit 1
  }

  local cmd="$1"
  shift
  local release
  release="$(normalize_release_version "$1")"
  shift

  case "$cmd" in
    prepare)
      prepare_release "$release" "$@"
      ;;
    finalize)
      finalize_release "$release" "$@"
      ;;
    *)
      usage
      die "Unknown command: $cmd"
      ;;
  esac
}

main "$@"
