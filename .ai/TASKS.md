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
| T-001 | CI Pipeline + Release Workflow Rework | claude | codex | TBD | ready_for_review | CI workflow runs on push/PR (spotless, compile, test, JaCoCo, artifacts); release.yml skeleton on tag push; ai-release.sh PR body references CI; build-all.sh deleted; Maven caching active | Local validation passed: `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, `mvn -T 1C -q test`; workflows added for CI/release | review |
| T-002 | Sonar Integration | claude | TBD | TBD | ready_for_implement | sonar-maven-plugin configured; CI runs Sonar analysis; SonarCloud quality gate defined and reported on PRs; no blocker/critical findings | n/a | implement |
| T-003 | Test Coverage ≥ 80% | claude | TBD | TBD | ready_for_implement | Overall line coverage ≥80% (JaCoCo aggregate); each library module ≥70%; jacoco:check enforced in build; example modules excluded | n/a | implement |
| T-004 | Public Interface JavaDoc | claude | TBD | TBD | ready_for_implement | All 15 interfaces have class-level JavaDoc; all ~117 public methods have method-level JavaDoc (@param, @return, @throws); English only; spotless clean | n/a | implement |
| T-005 | Maven Central Preparation | claude | TBD | TBD | ready_for_implement | POM metadata complete (name, description, url, licenses, scm, developers); GPG signing in release profile; source+javadoc JARs attached; publishing plugin configured; examples excluded from deploy; release.yml completed; dry-run passes | n/a | implement |
