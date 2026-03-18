# Review — T-004: Skip Maven Central for selected versions

Status: **complete**

Review Round: **2**

Reviewed: 2026-03-18

---

## Verdict

`PASS`

---

## Findings

### Round 1 blocker — resolved in commit `f67160d`

`finalize_release()` did not reset `central.skipPublishing`, causing the `true` value to leak into the next development cycle. Fixed by adding `set_central_skip_publishing "false"` immediately after `mvn versions:set` in `finalize_release()`. The reset is included in the next-cycle commit before `git add -A`. `docs/RELEASE.md` claim is now accurate.

---

### Criteria

| # | Acceptance Criterion | Result |
|---|---|---|
| 1 | `ai-release.sh prepare` accepts `--skip-central` | ✅ Parsed in `prepare_release()` arg loop; usage string updated |
| 2 | `central.skipPublishing` set to `true` in POM before release commit | ✅ `set_central_skip_publishing "true"` called after `mvn versions:set`, before `git add -A` |
| 3 | Without flag, behavior unchanged | ✅ `skip_central` defaults to `"false"`; `set_central_skip_publishing` not called |
| 4 | Skip visible in release PR diff | ✅ POM change in release commit, included in `git add -A` before commit |
| 5 | No CI workflow changes | ✅ No changes to `ci.yml`; existing plugin reads `${central.skipPublishing}` |
| 6 | `docs/RELEASE.md` documents the flag | ✅ Four doc bullets added covering: flag usage, PR diff visibility, CI behaviour, finalize reset — finalize reset now accurate after fix |
| 7 | PR body indicates whether Central is skipped | ✅ `central_publish_status` (enabled/skipped), `central_publish_flow`, and `central_publish_automation` variables surface the state in three PR body sections |
| 8 | Build passes | ✅ Per handoff: bash -n, spotless, test-compile, full test suite all green |

## Required Fixes

None — blocker resolved in commit `f67160d`.
