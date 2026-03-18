# Review — T-004: Skip Maven Central for selected versions

Status: **complete**

Review Round: **1**

Reviewed: 2026-03-18

---

## Verdict

`FAIL`

---

## Findings

### [BLOCKER] `finalize_release()` does not reset `central.skipPublishing` — docs claim is false

**Severity:** blocking

`set_central_skip_publishing` is called exactly once in the script — at line 249 inside `prepare_release()`, to set `true`. It is **never called** in `finalize_release()`.

After a `--skip-central` release the sequence is:

1. `prepare`: POM gets `central.skipPublishing=true`, committed, merged to `main`.
2. `finalize`: checks out `main` (which carries `true`), creates a new branch, runs `mvn versions:set` (version tags only — does not touch `central.skipPublishing`), renders `.ai/*.md` and `ROADMAP.md` templates (also do not touch `pom.xml`), commits.
3. Result: next cycle's development POM still has `central.skipPublishing=true`.
4. Consequence: the next `prepare` (even without `--skip-central`) includes `central.skipPublishing=true` in the release commit, and CI silently skips Maven Central for that release too.

`docs/RELEASE.md` states: *"Because finalize resets the POM for the next cycle, any `central.skipPublishing=true` release commit does not carry over to subsequent versions."* This is factually incorrect given the current implementation.

**Required fix:** add `set_central_skip_publishing "false"` in `finalize_release()` after `mvn versions:set`, so the property is always reset to `false` at the start of a new development cycle regardless of whether the previous release used `--skip-central`.

---

### Passing criteria (for reference)

| # | Acceptance Criterion | Result |
|---|---|---|
| 1 | `ai-release.sh prepare` accepts `--skip-central` | ✅ Parsed in `prepare_release()` arg loop; usage string updated |
| 2 | `central.skipPublishing` set to `true` in POM before release commit | ✅ `set_central_skip_publishing "true"` called after `mvn versions:set`, before `git add -A` |
| 3 | Without flag, behavior unchanged | ✅ `skip_central` defaults to `"false"`; `set_central_skip_publishing` not called |
| 4 | Skip visible in release PR diff | ✅ POM change in release commit, included in `git add -A` before commit |
| 5 | No CI workflow changes | ✅ No changes to `ci.yml`; existing plugin reads `${central.skipPublishing}` |
| 6 | `docs/RELEASE.md` documents the flag | ✅ Four doc bullets added covering: flag usage, PR diff visibility, CI behaviour, finalize reset — but the finalize reset bullet is inaccurate (see blocker above) |
| 7 | PR body indicates whether Central is skipped | ✅ `central_publish_status` (enabled/skipped), `central_publish_flow`, and `central_publish_automation` variables surface the state in three PR body sections |
| 8 | Build passes | ✅ Per handoff: bash -n, spotless, test-compile, full test suite all green |

## Required Fixes

1. In `finalize_release()`, after `mvn versions:set -DnewVersion="$next_version" -DgenerateBackupPoms=false`, add:
   ```bash
   set_central_skip_publishing "false"
   ```
   This ensures `central.skipPublishing` is always `false` in the next cycle's POM, making the `docs/RELEASE.md` claim accurate.
