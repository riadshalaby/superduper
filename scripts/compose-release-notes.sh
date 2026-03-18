#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: scripts/compose-release-notes.sh <current-tag>" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

extract_release_notes() {
  local body="$1"
  printf '%s\n' "$body" | awk '
    {
      sub(/\r$/, "")
    }
    /^##[[:space:]]+Release Notes[[:space:]]*$/ {
      capture = 1
      next
    }
    capture && /^##[[:space:]]+/ {
      exit
    }
    capture {
      print
    }
  '
}

trim_blank_lines() {
  awk '
    {
      lines[++count] = $0
    }
    END {
      start = 1
      while (start <= count && lines[start] ~ /^[[:space:]]*$/) {
        start++
      }

      end = count
      while (end >= start && lines[end] ~ /^[[:space:]]*$/) {
        end--
      }

      for (i = start; i <= end; i++) {
        print lines[i]
      }
    }
  '
}

has_label() {
  local labels="$1"
  local needle="$2"
  printf '%s\n' "$labels" | grep -Fxq "$needle"
}

category_for_labels() {
  local labels="$1"

  if has_label "$labels" "skip-changelog" || has_label "$labels" "chore" || has_label "$labels" "test" || has_label "$labels" "ci" || has_label "$labels" "refactor"; then
    return 1
  fi

  if has_label "$labels" "breaking" || has_label "$labels" "breaking-change"; then
    printf 'Breaking Changes'
  elif has_label "$labels" "feat" || has_label "$labels" "feature" || has_label "$labels" "enhancement"; then
    printf 'Features'
  elif has_label "$labels" "fix" || has_label "$labels" "bugfix" || has_label "$labels" "bug"; then
    printf 'Bug Fixes'
  elif has_label "$labels" "perf" || has_label "$labels" "performance"; then
    printf 'Performance'
  elif has_label "$labels" "docs" || has_label "$labels" "documentation"; then
    printf 'Documentation'
  else
    printf 'Other Changes'
  fi
}

append_entry() {
  local category="$1"
  local title="$2"
  local number="$3"
  local notes="$4"
  local block
  block="$(printf '### #%s %s\n\n%s\n\n' "$number" "$title" "$notes")"

  case "$category" in
    "Breaking Changes")
      breaking_changes+="$block"
      ;;
    "Features")
      features+="$block"
      ;;
    "Bug Fixes")
      bug_fixes+="$block"
      ;;
    "Performance")
      performance+="$block"
      ;;
    "Documentation")
      documentation+="$block"
      ;;
    *)
      other_changes+="$block"
      ;;
  esac
}

print_section() {
  local title="$1"
  local content="$2"

  if [[ -n "$content" ]]; then
    printf '## %s\n\n%s' "$title" "$content"
  fi
}

main() {
  [[ $# -eq 1 ]] || {
    usage
    exit 1
  }

  require_cmd gh
  require_cmd jq

  local current_tag="$1"
  local previous_tag
  previous_tag="$(
    gh release list --exclude-drafts --limit 100 --json tagName,publishedAt \
      | jq -r --arg current_tag "$current_tag" '
          sort_by(.publishedAt)
          | reverse
          | map(select(.tagName != $current_tag))
          | .[0].tagName // empty
        '
  )"

  local release_boundary=""
  if [[ -n "$previous_tag" ]]; then
    release_boundary="$(gh release view "$previous_tag" --json publishedAt --jq '.publishedAt')"
  fi

  local prs_json
  prs_json="$(gh pr list --base main --state merged --limit 200 --json number,title,body,labels,mergedAt)"

  local filtered_prs
  filtered_prs="$(
    printf '%s' "$prs_json" | jq -c --arg release_boundary "$release_boundary" '
      sort_by(.mergedAt)
      | map(select($release_boundary == "" or .mergedAt >= $release_boundary))
      | .[]
    '
  )"

  local features=""
  local bug_fixes=""
  local performance=""
  local documentation=""
  local breaking_changes=""
  local other_changes=""

  local pr_json
  while IFS= read -r pr_json; do
    [[ -n "$pr_json" ]] || continue

    local number
    local title
    local body
    local labels
    local category
    local notes

    number="$(printf '%s' "$pr_json" | jq -r '.number')"
    title="$(printf '%s' "$pr_json" | jq -r '.title')"
    body="$(printf '%s' "$pr_json" | jq -r '.body // ""')"
    labels="$(printf '%s' "$pr_json" | jq -r '.labels[].name // empty')"

    category="$(category_for_labels "$labels" || true)"
    [[ -n "$category" ]] || continue

    notes="$(extract_release_notes "$body")"
    notes="$(printf '%s\n' "$notes" | sed 's/<!--[^>]*-->//g')"
    notes="$(printf '%s\n' "$notes" | trim_blank_lines)"

    [[ -n "$notes" ]] || continue

    append_entry "$category" "$title" "$number" "$notes"
  done <<<"$filtered_prs"

  local output=""
  output+="$(print_section "Breaking Changes" "$breaking_changes")"
  output+="$(print_section "Features" "$features")"
  output+="$(print_section "Bug Fixes" "$bug_fixes")"
  output+="$(print_section "Performance" "$performance")"
  output+="$(print_section "Documentation" "$documentation")"
  output+="$(print_section "Other Changes" "$other_changes")"

  printf '%s' "$output" | trim_blank_lines
}

main "$@"
