# TASKS

Use this board to coordinate manual handoff between planner, implementer, and reviewer.

Status values:
- `todo`
- `in_planning`
- `ready_for_implement`
- `in_implementation`
- `ready_for_review`
- `in_review`
- `changes_requested`
- `done`
- `blocked`

| Task ID | Scope | Planner Agent | Implementer Agent | Reviewer Agent | Status | Acceptance Criteria | Evidence | Next Role |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| T-001 | CI Pipeline + Release Workflow Rework | claude | codex | claude | done | CI workflow runs on push/PR (spotless, compile, test, JaCoCo, artifacts); release.yml skeleton on tag push; ai-release.sh PR body references CI; build-all.sh deleted; Maven caching active | Local validation passed: `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, `mvn -T 1C -q test`; workflows added for CI/release | — |
| T-002 | Sonar Integration | claude | codex | claude | done | sonar-maven-plugin configured; CI runs Sonar analysis on `main` push; SonarCloud quality gate defined on `main` (free plan — branch/PR analysis not available); no blocker/critical findings | Local validation passed: `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, `mvn -T 1C -q test`; CI Sonar job runs on push to `main` only (free plan constraint); release.yml verify-main-ci gate blocks publish if Sonar failed; live SonarCloud quality gate definition and first-scan findings are deferred runtime steps | — |
| T-003 | Test Coverage ≥ 80% | claude | codex | claude | done | Overall line coverage ≥80% (JaCoCo aggregate); each library module ≥70%; jacoco:check enforced in build; example modules excluded | Aggregate line coverage 89.26%; `repository-api` 93.51%; `starter-autoselect` 82.34%; `worker-blocking` 85.22%; validations passed: `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, `mvn -T 1C -q test`, `mvn -q -DskipTests verify` | — |
| T-004 | Public Interface JavaDoc | claude | codex | TBD | ready_for_review | All 15 interfaces have class-level JavaDoc; all ~117 public methods have method-level JavaDoc (@param, @return, @throws); English only; spotless clean | JavaDoc added to all 15 public interfaces across observability, repository, JDBC/R2DBC dialect, and worker APIs; validations passed: `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, `mvn -T 1C -q test` | review |
| T-005 | Maven Central Preparation | claude | TBD | TBD | ready_for_implement | POM metadata complete (name, description, url, licenses, scm, developers); GPG signing in release profile; source+javadoc JARs attached; publishing plugin configured; examples excluded from deploy; release.yml completed; dry-run passes | n/a | implement |
