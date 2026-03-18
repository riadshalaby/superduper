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
| T-001 | Exclude test sources from Sonar analysis | claude | codex | claude | done | sonar.exclusions in parent POM; docs updated; build passes; SonarCloud confirms no test files in prod analysis | `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test` | — |
| T-002 | Structured release notes via release-drafter | claude | codex | claude | done | release-drafter workflow + config; aligned release.yml; PR template with labels; CI publishes draft; docs updated; build passes | `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test` | — |
| T-003 | Release notes from PR body sections | claude | codex | claude | done | compose-release-notes.sh extracts PR Release Notes sections; CI uses composed notes with fallback chain; docs updated; build passes | `bash -n scripts/compose-release-notes.sh`; `scripts/compose-release-notes.sh v0.6.4-preview`; `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test` | — |
| T-004 | Skip Maven Central for selected versions | claude | codex | claude | ready_for_review | ai-release.sh --skip-central flips central.skipPublishing; visible in PR diff; docs updated; build passes | `bash -n scripts/ai-release.sh`; `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test`; live `prepare --skip-central` not run locally to avoid creating a real PR/push | review |
