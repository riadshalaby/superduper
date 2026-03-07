#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <from-tag> <to-tag>" >&2
  echo "Example: $0 v0.2.0 v0.3.0" >&2
  exit 1
fi

FROM_TAG="$1"
TO_TAG="$2"
RANGE="${FROM_TAG}..${TO_TAG}"

for ref in "$FROM_TAG" "$TO_TAG"; do
  if ! git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    echo "Unknown git ref: ${ref}" >&2
    exit 1
  fi
done

COMMITS="$(git log --no-merges --format='%s' "${RANGE}")"

if [[ -z "${COMMITS}" ]]; then
  cat <<EOF
# Release Notes

## Changes

No commits found between ${FROM_TAG} and ${TO_TAG}.
EOF
  exit 0
fi

printf '%s\n' "$COMMITS" | awk -v from="$FROM_TAG" -v to="$TO_TAG" '
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

  title["feat"] = "Features"
  title["fix"] = "Fixes"
  title["perf"] = "Performance"
  title["refactor"] = "Refactors"
  title["docs"] = "Documentation"
  title["test"] = "Tests"
  title["build"] = "Build"
  title["ci"] = "CI"
  title["chore"] = "Chores"
  title["revert"] = "Reverts"
  title["other"] = "Other"
}
{
  subject = $0
  type = "other"
  summary = subject

  if (subject ~ /^[a-z]+(\([^)]+\))?(!)?: /) {
    type = subject
    sub(/[^a-z].*/, "", type)
    summary = subject
    sub(/^[a-z]+(\([^)]+\))?(!)?: /, "", summary)
  }

  entries[type] = entries[type] "- " summary "\n"
}
END {
  print "# Release Notes"
  print ""
  print "Generated from `" from ".." to "`."
  print ""

  for (i = 1; i <= order_count; i++) {
    type = order[i]
    if (entries[type] != "") {
      print "## " title[type]
      printf "%s", entries[type]
      print ""
    }
  }

  if (entries["other"] != "") {
    print "## " title["other"]
    printf "%s", entries["other"]
    print ""
  }
}
'
