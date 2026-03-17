# Review — T-001: Release Trigger Consistency + Release Notes

- **Verdict:** PASS
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-17T13:40Z
- **Commit reviewed:** `3b9f6d0` — `ci: make CI the sole release trigger`

---

## Findings (ordered by severity)

### INFO — `.github/release.yml` categories are label-based; this project uses Conventional Commits without PR labels

**File:** `.github/release.yml`

The `--generate-notes` flag uses GitHub's release notes generator, which applies the category rules in `.github/release.yml`. The categories are configured by PR labels (`feat`, `fix`, `perf`, `*`). Since this project uses Conventional Commits in commit subjects rather than GitHub PR labels, most items will fall through to "Other Changes". The plan explicitly acknowledged this limitation and accepted it: "The auto-generated notes from commit messages are already useful and consistently formatted." No action needed; the behaviour is intentional.

**No action required.**

---

### INFO — `release` job checkout uses default `fetch-depth: 1`; no full history needed

**File:** `ci.yml` — `release` job, checkout step (line 248)

Unlike `build` and `sonar`, the `release` job checkout does not set `fetch-depth: 0`. This is intentional: the `release` job only runs `mvn -Prelease deploy` and `gh release create`, neither of which requires git history. The tag already exists on origin (created by `tag-version`). No issue.

**No action required.**

---

## Plan Compliance Checklist

| Plan step | Expected | Actual | Status |
|-----------|----------|--------|--------|
| **AC 1** — No `git tag` in `finalize` | Removed | Absent from `finalize_release()` | PASS |
| **AC 1** — No `git push origin "$tag"` in `finalize` | Removed | Absent from `finalize_release()` | PASS |
| **AC 1** — Tag verified via `git ls-remote --tags origin` | Present | Lines 361–363 | PASS |
| **AC 1** — Fail message matches plan spec | "Tag v<X.Y.Z> not found on origin. Wait for CI…" | Exact match at line 362 | PASS |
| **AC 2** — `gh release create` step in CI `release` job | After Maven Central publish | Lines 275–284 | PASS |
| **AC 2** — `--generate-notes` flag | Present | Line 283 | PASS |
| **AC 2** — `--verify-tag` flag | Present | Line 284 | PASS |
| **AC 2** — `GITHUB_TOKEN` wired | `env: GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}` | Lines 276–277 | PASS |
| **AC 3** — Release notes from commit history | `--generate-notes` uses commits since previous tag | Mechanism in place | PASS |
| **Step 3** — `release` job `permissions: contents: write` | Required for `gh release create` | Line 239 | PASS |
| **Step 4** — PR body updated: automated post-merge flow | Describes CI creating tag, publishing, creating GitHub Release | Lines 294–297 | PASS |
| **Step 4** — `## Release Checklist` section removed | Removed entirely | Absent from PR body | PASS |
| **Step 6 — `.github/release.yml`** | Created with label-based categories | Present, matches plan spec | PASS |
| **AC 5** — `finalize` fails clearly if tag missing | `die "Tag $tag not found on origin…"` | Lines 361–363 | PASS |
| **AC 6** — PR body reflects deterministic post-merge flow | `after merge, ci.yml on main will automatically…` | Lines 294–297 | PASS |
| **AC 7** — `CLAUDE.md` CI Pipeline section: CI sole tag creator | "CI on `main` is the sole creator of release tags and GitHub Releases." | Line 28 | PASS |
| **AC 7** — `CLAUDE.md` CI Pipeline section: `finalize` read-only on tags | "`finalize` no longer pushes tags…" | Line 29 | PASS |
| **AC 7** — `CLAUDE.md` Release Rules Phase 2: tag verification | "verifies CI has created/pushed tag vX.Y.Z" | Line 134 | PASS |
| **AC 7** — `CLAUDE.md` Release Rules Phase 3: CI post-merge actions | New bullet describing CI creates tag, publishes, creates release | Lines 135–136 | PASS |
| **AC 7** — `CLAUDE.md` Release Safety: no manual tags | "Never create release tags manually; CI on `main` is the sole tag creator." | Line 145 | PASS |

---

## Quality Assessment

**`finalize` tag verification:** The transition from "create-or-skip" to "verify-or-die" is clean. `git fetch origin --tags` at line 352 refreshes the local tag cache before the `git ls-remote` remote check, providing a consistent state. The error message is actionable — it tells the user exactly what to wait for and what to do next.

**`gh release create` placement:** The GitHub Release step is correctly sequenced after "Publish to Maven Central". If the Maven Central publish fails, no GitHub Release is created. This preserves the correct publish-then-announce order.

**`--verify-tag` guard:** The `--verify-tag` flag ensures `gh release create` refuses to create a release pointing at a non-existent tag. Given that `tag-version` already pushed the tag before this job runs, this is a belt-and-suspenders guard against unexpected race conditions or workflow misconfiguration.

**`.github/release.yml` categorization:** The label-based categories are correctly placed at `.github/release.yml` (not inside `.github/workflows/`). GitHub's release notes generator reads this file automatically. For Conventional Commits projects the "Other Changes" bucket will capture most entries, but this is documented in the plan as acceptable.

**`CLAUDE.md`:** All three sections (CI Pipeline, Release Rules, Release Safety) are updated accurately and concisely. The new "Phase 3" bullet in Release Rules makes the three-phase nature of the workflow explicit without requiring the user to read the CI YAML.

**PR body:** The old `## Release Checklist` section (which duplicated what CI enforces) is removed. The new "after merge, ci.yml on main will automatically" block gives a clear user-facing description of the automated post-merge actions.

---

## Required Fixes

None. All acceptance criteria are satisfied.

---

## Summary

Tag creation is now strictly owned by CI: `finalize` was stripped of `git tag` and `git push origin "$tag"`, replacing that block with a `git ls-remote` verification that fails fast with a clear error if CI hasn't run yet. The `release` job gained `contents: write` permission and a `gh release create --generate-notes --verify-tag` step that fires after Maven Central publish. `.github/release.yml` provides label-based categorization for future use. `CLAUDE.md` and the `prepare` PR body accurately describe the deterministic post-merge flow. Verdict **PASS**.
