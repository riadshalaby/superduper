# Review — T-001: Unified CI Pipeline Consolidation

- **Verdict:** PASS
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-17T13:10Z
- **Commit reviewed:** `cd9d619` — `ci: unify release flow in the main workflow`

---

## Findings (ordered by severity)

### INFO — Sonar quality gate poll window is capped at 60 seconds (12 × 5 s)

**File:** `ci.yml` — "Check Sonar quality gate" step (the retry loop)

The background CE task is polled up to 12 times with a 5-second sleep, giving a 60-second window before the step exits with a warning. For larger codebases Sonar analysis can exceed this. Because `continue-on-error: true` is set, a timeout produces an orange warning step and does not fail the workflow. This is intentional non-blocking behaviour and the plan does not specify a minimum poll window.

**No action required.**

---

### INFO — `concurrency: cancel-in-progress: true` covers `tag-version` and `release`

**File:** `ci.yml` — top-level `concurrency` block

The concurrency group is scoped to `github.ref`, so two rapid pushes to `main` would cancel the earlier run — potentially mid-`tag-version` or mid-`release`. In practice release merges happen infrequently and the `tag-version` job is idempotent (remote tag check), so a cancelled-then-re-run sequence is safe. This pattern pre-dates T-001 and the plan does not require a change.

**No action required.**

---

### INFO — `release` job re-verifies POM version against `tag-version` output

**File:** `ci.yml` — "Verify release version" step (lines 261–263)

The release job re-evaluates the project version with `mvn help:evaluate` and asserts it matches `tag-version.outputs.version`. This is an extra correctness guard beyond the plan's specification, ensuring no drift between the tagged ref and the checked-out POM. A good defensive addition.

**No action required.**

---

## Plan Compliance Checklist

| Plan step | Expected | Actual | Status |
|-----------|----------|--------|--------|
| **Step 1** — `-Dsonar.qualitygate.wait=true` removed | Removed from `mvn sonar:sonar` invocation | Absent from line 118 | PASS |
| **Step 1** — Separate quality gate check step | Post-analysis step with `continue-on-error: true` | Present (lines 120–167), `continue-on-error: true` | PASS |
| **Step 1** — Clear warning on red gate | `::warning::` annotation + dashboard link | Line 165: `::warning::Sonar quality gate FAILED...` | PASS |
| **Step 1** — Clear success message on green gate | Print PASSED + dashboard link | Line 161: `Sonar quality gate PASSED: $dashboard_url` | PASS |
| **Step 1** — `SONAR_TOKEN` guard on both Sonar steps | `if: env.SONAR_TOKEN != ''` | Present on lines 115 and 121 | PASS |
| **Step 2** — `tag-version` job added | `needs: [build]`; main-push only; outputs `is_release`, `version` | Present (lines 169–230) | PASS |
| **Step 2** — `release` job added | `needs: [build, tag-version]`; main-push only; `is_release == true` | Present (lines 232–273) | PASS |
| **Step 3** — `tag-version`: `permissions: contents: write` | Required to push tags | Set on lines 174–175 | PASS |
| **Step 3** — Version extracted via `mvn help:evaluate` | `mvn -q -DforceStdout help:evaluate -Dexpression=project.version` | Line 196 | PASS |
| **Step 3** — Snapshot skip | `if [[ "$version" == *-SNAPSHOT ]]` → `is_release=false` | Lines 199–203 | PASS |
| **Step 3** — Remote tag existence check (idempotent) | `git ls-remote --tags origin` | Lines 206–209 | PASS |
| **Step 4** — `build` on all pushes + PRs (unchanged) | `if: event_name == push or pull_request` | Line 17 | PASS |
| **Step 4** — `sonar` on main push only (unchanged) | `event_name == push && ref == refs/heads/main` | Line 85 | PASS |
| **Step 4** — `tag-version` restricted to main push | `event_name == push && ref == refs/heads/main` | Line 173 | PASS |
| **Step 4** — `release` restricted to main push + `is_release == true` | Full condition including both `result == success` checks | Line 237 | PASS |
| **Step 5** — `release.yml` deleted | File must not exist | Glob returns only `ci.yml` | PASS |
| **Step 6** — `ai-release.sh` PR body updated | References unified `ci.yml`; no `release.yml` mention | Lines 283–299: "unified GitHub Actions CI in ci.yml" | PASS |
| **Step 6** — `finalize` manual tag push preserved as safety backstop | Comment updated to note CI also tags | Line 372 comment | PASS |
| **Step 7** — `CLAUDE.md` CI Pipeline section updated | Unified model; Sonar non-blocking; release flow; `release.yml` banned | Lines 24–28 | PASS |
| **Acceptance 5** — Secrets verified before publish | `test -n` for all 4 secrets | Lines 265–270 | PASS |
| **Acceptance 5** — Publish command | `mvn -B -Prelease -DskipTests deploy` | Line 273 | PASS |
| **Acceptance 5** — `setup-java` Central credentials wired | `server-id: central`, `server-username/password` env var names | Lines 255–258 | PASS |

---

## Quality Assessment

**Sonar non-blocking design:** The two-step pattern — run `sonar:sonar` without `qualitygate.wait`, then poll the CE task API independently with `continue-on-error: true` — is the correct approach. It separates analysis submission from quality gate checking, letting CI remain green while still surfacing gate failures as visible orange steps and `::warning::` annotations in the Actions UI.

**`tag-version` idempotency:** Two independent checks prevent duplicate tag creation: (1) `git ls-remote --tags origin` in the metadata step sets `is_release=false` if the tag exists on the remote, and (2) `git rev-parse --verify --quiet` in the push step guards against local duplicates. The `git push origin "$tag"` therefore handles the case where a manual `finalize` already pushed the tag, failing gracefully.

**`release` condition:** `needs.build.result == 'success' && needs.tag-version.result == 'success' && ... && needs.tag-version.outputs.is_release == 'true'` covers all required guards: successful build, successful tagging step, main-push context, and explicit release flag. Snapshot pushes to main will have `tag-version` exit with `is_release=false`, causing `release` to be skipped.

**`ai-release.sh` updates:** The PR body now accurately describes the post-merge pipeline ("tag-version release tag creation", "release Maven Central publish") without any reference to the deleted `release.yml`. The `finalize` comment is clear that the manual tag push is a safety backstop, not the primary tagging mechanism.

**`CLAUDE.md`:** Concise and accurate — all CI/CD in one file, Sonar non-blocking, release flow noted, `release.yml` prohibited.

**`ROADMAP.md`:** Reflects the three v0.6.2 priorities that this commit fully addresses.

---

## Required Fixes

None. All acceptance criteria are satisfied.

---

## Summary

The unified CI pipeline is correctly implemented. `ci.yml` now contains all four jobs (`build`, `sonar`, `tag-version`, `release`) with strict `needs`-based sequencing. Sonar is non-blocking with explicit gate-status logging. `tag-version` is idempotent and skips snapshots. `release` is gated on successful tagging with a pre-publish secrets check. `release.yml` is deleted. `ai-release.sh` and `CLAUDE.md` are updated to match the new model. Verdict **PASS**.
