# Review — T-001: CI Pipeline + Release Workflow Rework

- **Verdict:** PASS
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-16T19:15Z
- **Commit reviewed:** d8e9de0 — `ci: add GitHub Actions workflows`

---

## Findings (ordered by severity)

### INFO — JaCoCo report step is a separate Maven invocation

**File:** `.github/workflows/ci.yml` (step `Generate JaCoCo reports`)

The workflow runs `mvn jacoco:report` as a separate invocation after `mvn -T 1C test`. This is the pattern prescribed in the plan and works correctly as long as the JaCoCo `prepare-agent` goal is bound to the Maven `initialize` phase in the POM (the standard configuration). If the POM uses the surefire/jacoco lifecycle binding correctly, instrumented `.exec` data will be present from the test run and `jacoco:report` will read it. This is low-risk but worth verifying when coverage drops unexpectedly in CI.

**No action required.**

### INFO — No `permissions` block in workflow files

**Files:** `.github/workflows/ci.yml`, `.github/workflows/release.yml`

Neither workflow declares a `permissions` block. GitHub Actions defaults to `read` on all tokens for organization repos with the default permission setting. Since neither workflow performs write operations (no token push, no release asset upload, no PR comments), the default is sufficient today. When T-005 completes the publish step, a `permissions: contents: write` block will be needed in `release.yml`.

**No action required for T-001; T-005 must add it when implementing publish steps.**

### INFO — release.yml only compiles (no full test run)

**File:** `.github/workflows/release.yml`

The release skeleton runs `mvn -q -DskipTests test-compile` only. This is the exact behavior specified in the plan ("verify the tag builds"). Full tests are the responsibility of the CI workflow that ran on the release PR branch before merge. The comment in `ai-release.sh` finalize (`# Tag pushes trigger the release GitHub Actions workflow for publish steps.`) makes this flow clear.

**No action required.**

---

## Plan Compliance Checklist

| Plan step | Expected | Actual | Status |
|-----------|----------|--------|--------|
| Delete `scripts/build-all.sh` | File absent | Not in repo | PASS |
| `ci.yml` — push trigger all branches | `push: branches: '**'` | Present | PASS |
| `ci.yml` — PR trigger to main | `pull_request: branches: main` | Present | PASS |
| `ci.yml` — ubuntu-latest | ubuntu-latest | Present | PASS |
| `ci.yml` — JDK 25 / temurin | java-version 25, temurin | Present | PASS |
| `ci.yml` — Maven cache | `cache: maven` | Present | PASS |
| `ci.yml` — spotless:check | `mvn -q spotless:check` | Present | PASS |
| `ci.yml` — compile gate | `mvn -q -DskipTests test-compile` | Present | PASS |
| `ci.yml` — full test suite | `mvn -T 1C test` | Present | PASS |
| `ci.yml` — JaCoCo report | `mvn jacoco:report` | Present | PASS |
| `ci.yml` — upload surefire reports | upload-artifact `if: always()` | Present | PASS |
| `ci.yml` — upload JaCoCo reports | upload-artifact `if: always()` | Present | PASS |
| `ci.yml` — concurrency/cancel | group + cancel-in-progress | Present | PASS |
| `release.yml` — tag trigger `v*` | `push: tags: v*` | Present | PASS |
| `release.yml` — test-compile step | Present | Present | PASS |
| `release.yml` — publish placeholder | echo placeholder | Present | PASS |
| `ai-release.sh` — CI status section in PR body | CI checks listed | Lines 283–296 | PASS |
| `ai-release.sh` — local pre-flight kept | test-compile + test | Lines 228–230 | PASS |
| `CLAUDE.md` — `## CI Pipeline` section | Added | Lines 22–26 | PASS |
| `CLAUDE.md` — push/PR trigger noted | Present | Present | PASS |
| `CLAUDE.md` — release tag triggers workflow | Present | Present | PASS |
| `CLAUDE.md` — build-all.sh removal noted | Present | Present | PASS |

---

## Required Fixes

None. All acceptance criteria are satisfied.

---

## Summary

The implementation is complete and correct. All five plan steps are implemented as specified. The CI workflow covers the full gate chain (format → compile → test → coverage → artifact upload) with proper concurrency cancellation. The release skeleton is minimal and correct. The `ai-release.sh` PR body now references CI as the authoritative gate. CLAUDE.md is updated with CI guidance. No blocking findings.
