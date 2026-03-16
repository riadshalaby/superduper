# Review — T-002: Sonar Integration

- **Verdict:** PASS_WITH_NOTES
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-16T19:45Z
- **Commits reviewed:** 75c1614, 83ee6a6, c14e2a0, c968e71, 11fb0d7

---

## Findings (ordered by severity)

### NOTE (medium) — Sonar runs on main-only push; no PR-level quality gate feedback

**File:** `.github/workflows/ci.yml` (sonar job condition, line 81)
**Criterion:** "SonarCloud quality gate status is reported on PRs to `main`"

The Sonar job condition is:
```yaml
if: ${{ needs.build.result == 'success' && github.event_name == 'push' && github.ref == 'refs/heads/main' }}
```

This means Sonar only runs after merging to `main`, never on PRs or feature branches. The plan acceptance criteria explicitly requires "SonarCloud quality gate status is reported on PRs to `main`" — this is not met.

**Mitigating factors:**
- `sonar.qualitygate.wait=true` is configured, so a failing quality gate will fail the `main` push CI run.
- The new `verify-main-ci` job in `release.yml` blocks publishing if the CI (including Sonar) did not succeed on main — meaning a quality gate failure on main prevents release.
- Running Sonar on PRs from same-repo branches (using `pull_request_target`) would expose `SONAR_TOKEN` to PR code in the same repo, which is a valid security concern.

**Assessment:** The main-only Sonar approach is a valid, safer trade-off, but it deviates from a documented acceptance criterion. PR authors get no Sonar feedback before merge. This is noted for awareness; it does not block this task if the main-gate approach is acceptable to the user. Recommend updating the acceptance criterion in TASKS.md to reflect the chosen approach, or plan a follow-up to add PR-level Sonar via `pull_request_target` with a careful permissions model.

**No required fix; user decision on acceptable trade-off.**

---

### NOTE (low) — TASKS.md evidence claim does not match final implementation

**File:** `.ai/TASKS.md` (T-002 Evidence column)

The evidence column states: *"CI Sonar step added for push and same-repo PRs"*. The final implementation runs Sonar only on `push` to `main`, not on PRs. The evidence description was written for an earlier iteration and was not updated after the approach was narrowed to main-only.

**No required fix for the code; the evidence description should be corrected in TASKS.md.**

---

### NOTE (low) — External runtime requirements not verifiable in code

**Plan criteria that require live SonarCloud configuration:**
1. "SonarCloud quality gate is defined and visible on the project dashboard"
2. "No blocker or critical bugs/vulnerabilities in the existing codebase"

These require an active SonarCloud account, a completed first scan, and quality gate configuration via the SonarCloud UI. They cannot be verified by static code review. The implementer correctly noted "live SonarCloud upload not run in-session."

**No action required at code review time; operationally required before T-002 can be considered fully closed.**

---

### INFO — German comments in pom.xml (pre-existing, not introduced by T-002)

**File:** `pom.xml` (lines 203, 205 in the spotless plugin configuration)

```xml
<!-- In CI wollen wir failen, wenn Format nicht passt -->
<!-- lokales Auto-Fix optional als eigenes Execution/Profil -->
```

These German comments violate the CLAUDE.md rule "Use English for code comments." They pre-date T-002 (present in commit `d8e9de0`). T-002 is not responsible for them, but they represent existing technical debt to address in a cleanup task.

**Not a T-002 issue; tracked as pre-existing debt.**

---

### INFO — byte-buddy-agent added to parent POM `<dependencies>` (not `<dependencyManagement>`)

**File:** `pom.xml` (commit `c14e2a0`)

```xml
<dependencies>
  <dependency>
    <groupId>net.bytebuddy</groupId>
    <artifactId>byte-buddy-agent</artifactId>
    <version>${byte-buddy.version}</version>
    <scope>test</scope>
  </dependency>
</dependencies>
```

Placing the agent in `<dependencies>` (not `<dependencyManagement>`) means it is inherited by all child modules. This is intentional — the surefire `argLine` references it by path at test time, so it must be present in every module's local repository before the test phase. The `<scope>test</scope>` limits transitive leakage. Valid pattern for a JVM agent required at test runtime.

**No action required.**

---

### INFO — `release.yml` bonus: verify-main-ci gate (beyond T-002 plan scope)

**File:** `.github/workflows/release.yml` (commit `11fb0d7`)

The release workflow now has a `verify-main-ci` job that uses `actions/github-script` to verify a successful CI run exists for the tagged main commit before proceeding to the publish job. This is a positive addition not in the T-002 plan that significantly strengthens the release safety guarantee.

**Positive finding; no action required.**

---

## Plan Compliance Checklist

| Plan step | Expected | Actual | Status |
|-----------|----------|--------|--------|
| `sonar-maven-plugin` in `<pluginManagement>` | Present | Present (version 5.5.0.6356) | PASS |
| `sonar.organization` property | `rsworld` | `rsworld` | PASS |
| `sonar.host.url` property | `https://sonarcloud.io` | `https://sonarcloud.io` | PASS |
| `sonar.projectKey` property | `rsworld_superduper` | `rsworld_superduper` | PASS |
| `sonar.java.coveragePlugin` property | `jacoco` | `jacoco` | PASS |
| `sonar.coverage.jacoco.xmlReportPaths` property | Per-module jacoco.xml | `**/target/site/jacoco/jacoco.xml` | PASS |
| Example modules excluded from Sonar | `sonar.skip` or exclusions | `<sonar.skip>true</sonar.skip>` in all 5 example POMs | PASS (pre-existing) |
| Sonar step in CI workflow | Present | Separate `sonar` job | PASS |
| Sonar runs on push events | Present | Push to main only | PARTIAL |
| Sonar not triggered on forked PRs | Secrets safe | main-only enforced | PASS |
| `SONAR_TOKEN` from secrets | Present | `secrets.SONAR_TOKEN` | PASS |
| Quality gate waiting | `sonar.qualitygate.wait=true` | Present | PASS |
| PR quality gate reporting | "reported on PRs to main" | Not implemented | NOTE |
| Live SonarCloud quality gate defined | External | Cannot verify | DEFERRED |
| No blocker/critical findings | External | Cannot verify | DEFERRED |

---

## Required Fixes

None that block verdict. The deviations are noted and the main-gate approach provides meaningful protection. Recommend:

1. **Update TASKS.md T-002 evidence** to accurately state "CI Sonar step added for push to `main`; PR-level quality gate feedback not implemented" instead of "for push and same-repo PRs."
2. **Operationally:** Configure SonarCloud quality gate and verify first scan results before declaring T-002 fully done.

---

## Summary

The Sonar integration is structurally complete and correctly configured: properties, plugin version management, `sonar.qualitygate.wait`, and artifact sharing between jobs are all solid. The main-only Sonar restriction is a defensible security trade-off, and the new `verify-main-ci` release gate compensates by blocking publish if the post-merge Sonar run failed. The gap between the planned "reported on PRs" criterion and the main-only implementation is the primary deviation. Verdict **PASS_WITH_NOTES** — no blocking findings, but the PR feedback gap and the TASKS.md evidence mismatch should be noted.
