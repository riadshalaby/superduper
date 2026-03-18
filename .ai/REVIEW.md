# Review — T-003: Release notes from PR body sections

Status: **complete**

Review Round: **2**

Reviewed: 2026-03-18

---

## Verdict

`PASS`

---

## Findings

All eight acceptance criteria from `.ai/PLAN.md` are satisfied. No issues found.

| # | Acceptance Criterion | Result |
|---|---|---|
| 1 | `scripts/compose-release-notes.sh` exists and is executable | ✅ `-rwxr-xr-x`; `#!/usr/bin/env bash` shebang present |
| 2 | Script extracts content from `## Release Notes` section of merged PR bodies | ✅ `extract_release_notes()` uses `awk` to capture between `## Release Notes` and next `##` heading; strips `\r`; HTML comments stripped via `perl -0pe 's/<!--.*?-->//gs'` |
| 3 | Entries grouped by category labels matching release-drafter config | ✅ `category_for_labels()` maps all five categories (Breaking Changes, Features, Bug Fixes, Performance, Documentation) plus "Other Changes" fallback; label set is identical to `release-drafter.yml` |
| 4 | PRs with empty release notes or noise labels are omitted | ✅ Noise labels: `category_for_labels` returns exit 1 → empty `$category` → `continue`. Empty notes: `[[ -n "$notes" ]] || continue` |
| 5 | CI `Create GitHub Release` step calls script and uses its output as release body | ✅ `notes="$(bash scripts/compose-release-notes.sh "$tag" 2>/dev/null \|\| true)"` then `--notes "$notes"` |
| 6 | Fallback chain: composed notes → release-drafter draft → `--generate-notes` | ✅ Implementation correctly handles the case where both notes and a draft exist: edits the draft with composed notes (cleaner than plan sketch, no double-release risk) |
| 7 | `docs/RELEASE.md` updated with new workflow | ✅ Documents script name/location, extraction from `## Release Notes` sections, omission of empty PRs, three-tier fallback |
| 8 | Build compiles and tests pass | ✅ Per handoff: `bash -n`, preview smoke-test, spotless, test-compile, full test suite all green |

### Notes

- The implementation introduces `perl` as a runtime dependency (for `<!--...-->` stripping) with `require_cmd perl`. `perl` is available on `ubuntu-latest` CI runners. The plan only specified "strip HTML comments" without mandating a tool; this is acceptable.
- The CI step fetches `draft_tag` before calling the script. When composed notes are produced and a draft also exists, the implementation edits the draft with `--notes "$notes"` rather than creating a new release. This is a deliberate improvement over the plan sketch — it avoids creating a release when a draft for the same cycle already exists, preventing a potential conflict.
- The plan's algorithmic sketch used `declare -A CATEGORY_HEADINGS`; the implementation replaces this with the `category_for_labels()` function. The function is cleaner and equivalent. Not a deviation.
- Output section ordering (Breaking Changes first, then Features, Bug Fixes, Performance, Documentation, Other Changes) is consistent with standard changelog convention.
- ~~**Suggestion (non-blocking):** The `perl` dependency (line 190) can be replaced by `sed 's/<!--[^>]*-->//g'`, removing the `require_cmd perl` guard.~~ **Resolved in commit `d1ab8bd`** — `require_cmd perl` removed; `perl -0pe` replaced with `sed 's/<!--[^>]*-->//g'`. Script now depends only on `gh`, `jq`, `awk`, and `sed`.

## Open Questions

None.

## Required Fixes

None.
