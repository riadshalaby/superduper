# Plan — 0.6.1

Status: **active**

Goal: make `v0.6.1` release-ready with CI pipeline, static analysis, ≥80% test coverage, documented public interfaces, and Maven Central publishing preparation.

---

## Task Overview

| Task ID | Scope | Dependencies | Estimated Size |
|---------|-------|-------------|----------------|
| T-001 | CI Pipeline + Release Workflow Rework | none | large |
| T-002 | Sonar Integration | T-001 | medium |
| T-003 | Test Coverage ≥ 80% | T-001 | large |
| T-004 | Public Interface JavaDoc | none | medium |
| T-005 | Maven Central Preparation | T-001 | medium |

---

## T-001 — CI Pipeline + Release Workflow Rework

### Scope
Establish a GitHub Actions CI pipeline for push and pull_request events on all branches. Rework the existing release scripts to be CI-aware. Delete the stale `build-all.sh`.

### Current State
- No `.github/workflows/` directory exists.
- `scripts/build-all.sh` is a stale Go build script (gohour) — must be deleted.
- `scripts/ai-release.sh` runs all validation locally; PR body uses manual checkboxes with no CI references.

### Implementation Steps

#### 1. Delete stale script
- Delete `scripts/build-all.sh`.

#### 2. Create CI workflow: `.github/workflows/ci.yml`
- Trigger on `push` (all branches) and `pull_request` (to `main`).
- Use GitHub-hosted runners (`ubuntu-latest`).
- Java setup: JDK 25 (matching `java.version` property). Use `actions/setup-java` with `temurin` distribution.
- Maven dependency caching: use `actions/setup-java` cache option (`cache: maven`) or `actions/cache` for `~/.m2/repository`.
- Steps:
  1. Checkout code.
  2. Setup Java + cache.
  3. `mvn -q spotless:check` — format gate.
  4. `mvn -q -DskipTests test-compile` — compile gate.
  5. `mvn -T 1C test` — full test suite.
  6. `mvn jacoco:report` — generate coverage reports (if not already done by `verify`).
  7. Upload test reports (`target/surefire-reports`) and JaCoCo reports (`target/site/jacoco`) as workflow artifacts.
- Concurrency: cancel in-progress runs for the same branch/PR.

#### 3. Create release publish workflow: `.github/workflows/release.yml`
- Trigger on tag push matching `v*`.
- Steps:
  1. Checkout code.
  2. Setup Java.
  3. `mvn -q -DskipTests test-compile` — verify the tag builds.
  4. Placeholder step for Maven Central publish (implemented in T-005).
- This workflow is a skeleton now; T-005 fills in the signing and publishing steps.

#### 4. Rework `scripts/ai-release.sh`
- In `prepare_release()`:
  - Update the PR body template to include a CI status section referencing GitHub Actions checks (Sonar, coverage, tests, spotless).
  - Keep local `test-compile` + `test` as a pre-flight sanity check, but make it clear in the PR body that CI is the authoritative gate.
- In `finalize_release()`:
  - No structural changes needed; the tag push will trigger `release.yml` automatically.
- Update inline comments to reflect CI-aware flow.

#### 5. Update `CLAUDE.md`
- Add CI-related notes under a new `## CI Pipeline` section:
  - CI runs on push/PR.
  - Release tags trigger the publish workflow.
  - Reference `build-all.sh` removal.

### Acceptance Criteria
- `scripts/build-all.sh` is deleted.
- `.github/workflows/ci.yml` runs successfully on push and pull_request, executing: spotless check, compile, test, JaCoCo report, artifact upload.
- `.github/workflows/release.yml` exists as a skeleton triggered on `v*` tag push.
- `scripts/ai-release.sh` PR body references CI status checks.
- Maven dependency caching reduces CI runtime on subsequent runs.
- Test and coverage artifacts are downloadable from the Actions tab.

### Validation
- Push the branch and verify the CI workflow triggers and passes.
- Open a draft PR to main and verify CI runs on the PR.
- `mvn -q -DskipTests test-compile` locally.

---

## T-002 — Sonar Integration

### Scope
Integrate SonarQube/SonarCloud analysis into the build (local + CI) and define enforceable quality gates.

### Current State
- No `sonar-maven-plugin`, no `sonar-project.properties`, no Sonar configuration anywhere.

### Implementation Steps

#### 1. Add SonarCloud project configuration
- Add `sonar-maven-plugin` to the parent POM `<pluginManagement>`.
- Add Sonar properties in the parent POM `<properties>`:
  - `sonar.organization` — the SonarCloud organization key.
  - `sonar.host.url` — `https://sonarcloud.io`.
  - `sonar.projectKey` — `rsworld_superduper` (or as configured).
  - `sonar.java.coveragePlugin=jacoco`.
  - `sonar.coverage.jacoco.xmlReportPaths` — point to each module's `target/site/jacoco/jacoco.xml`.
- Exclude example modules from Sonar analysis (`sonar.exclusions` or per-module `<properties>`).

#### 2. Add Sonar step to CI workflow
- In `.github/workflows/ci.yml`, add a Sonar analysis step after the test step:
  - `mvn -B sonar:sonar` with `SONAR_TOKEN` from GitHub secrets.
  - Run only on `push` events (not on PR from forks, since secrets are unavailable).
  - Or use `pull_request_target` for PRs from the same repo.
- Configure SonarCloud quality gate webhook or use the `sonarcloud-github-action` for PR decoration.

#### 3. Define quality gates on SonarCloud
- Configure quality gate rules on SonarCloud (via UI or API):
  - New code coverage ≥ 80%.
  - No new bugs (severity: blocker, critical).
  - No new vulnerabilities.
  - No new security hotspots (unreviewed).
  - Maintainability: new code smells rating A.
- Enable quality gate as a GitHub required status check on the `main` branch.

#### 4. Fix critical existing findings
- After first Sonar scan, triage findings.
- Fix any blocker or critical bugs/vulnerabilities in the existing codebase.
- Document accepted findings (if any) with `@SuppressWarnings` and justification.

### Acceptance Criteria
- `mvn sonar:sonar` runs locally and uploads results to SonarCloud.
- CI workflow includes Sonar analysis and publishes results on every push.
- SonarCloud quality gate is defined and visible on the project dashboard.
- SonarCloud quality gate status is reported on PRs to `main`.
- No blocker or critical bugs/vulnerabilities in the existing codebase (or documented exceptions).

### Validation
- Push the branch and verify Sonar analysis appears in CI logs.
- Check SonarCloud dashboard for project overview.
- Open a PR and verify quality gate status appears as a check.

---

## T-003 — Test Coverage ≥ 80%

### Scope
Reach and enforce ≥80% line coverage across the project by adding missing tests and configuring JaCoCo enforcement.

### Current State

| Module | Line Coverage | Gap to 80% |
|--------|-------------|------------|
| observability-metrics | 92% | — |
| repository-jdbc | 87% | — |
| observability-logging | 86% | — |
| observability-api | 85% | — |
| starter-autoselect | 66% | +14pp needed |
| repository-api | 47% | +33pp needed |
| worker-blocking | 20% | +60pp needed |
| worker-reactive | 22% | +58pp needed |
| repository-r2dbc | 9% | +71pp needed |
| consumer-kafka-blocking | 0% | +80pp needed |
| consumer-kafka-reactive | 0% | +80pp needed |

Total test files: 51 (mix of unit tests and Testcontainers integration tests).

### Implementation Steps

#### 1. Add JaCoCo aggregate report
- Add a `jacoco-report-aggregate` module or configure `jacoco:report-aggregate` in the parent POM to produce a single project-wide coverage report.
- This is needed for an accurate overall coverage number and for SonarCloud to consume a unified report.

#### 2. Add JaCoCo enforcement
- Add a `jacoco:check` execution in the parent POM:
  - Rule: `BUNDLE` counter `LINE` minimum `0.80`.
  - Exclude example modules from enforcement.
- This makes the build fail if coverage drops below 80%.

#### 3. Add tests — priority order (largest gaps first)

**consumer-kafka-blocking (0% → ≥80%)**
- Unit tests for `KafkaConsumerService`: mock the repository, verify ingest calls, metadata resolution, error handling.
- Unit test for `KafkaConsumerAutoConfiguration`: verify bean wiring with mock context.

**consumer-kafka-reactive (0% → ≥80%)**
- Mirror the blocking consumer tests with reactive types (`StepVerifier`).

**repository-r2dbc (9% → ≥80%)**
- Unit tests for `R2dbcMessageIngestRepository`, `R2dbcWorkerMessageRepository`, `R2dbcWorkerMaintenanceRepository`.
- Test SQL dialect methods for both Postgres and MariaDB R2DBC dialects.
- Use `mockito-reactor` or manual `Mono`/`Flux` stubs.

**worker-blocking (20% → ≥80%)**
- Unit tests for `TopicWorkerCoordinator`, `TopicWorkerInstance`, `HeartbeatService`, `OrphanReclaimer`, `CleanupService`, `RedriveService`, `QueueHealthService`.
- Mock repository ports and observer.
- Test claim-process-outcome lifecycle, per-key ordering, retry escalation, stop logic.

**worker-reactive (22% → ≥80%)**
- Mirror blocking worker tests using `StepVerifier`.
- Test `ReactiveTopicWorkerCoordinator`, `ReactiveTopicWorkerInstance`, `ReactiveHeartbeatService`, `ReactiveOrphanReclaimer`, `ReactiveCleanupService`, `ReactiveRedriveService`, `ReactiveQueueHealthService`.

**starter-autoselect (66% → ≥80%)**
- Test `AutoSelectConfiguration` edge cases: missing properties, fallback paths, multi-topic wiring.
- Test `TopicRegistry` and `RepositoryFactory` for shared vs dedicated table paths.

**repository-api (47% → ≥80%)**
- Test any concrete/default methods on the interfaces.
- Test `ConsumerMetadataResolver` implementations if any exist.

#### 4. Publish coverage reports as CI artifacts
- Already covered in T-001; verify JaCoCo XML + HTML reports are uploaded.

### Acceptance Criteria
- Overall project line coverage ≥ 80% (measured by JaCoCo aggregate report).
- Each library module (excluding examples) has line coverage ≥ 70% individually.
- `jacoco:check` is configured in the build and fails on coverage regression below 80%.
- All new tests pass in CI.
- Example modules are excluded from coverage enforcement.

### Validation
- `mvn -T 1C -q test` passes.
- `mvn verify` passes (includes JaCoCo check).
- JaCoCo aggregate report shows ≥80% line coverage.

---

## T-004 — Public Interface JavaDoc

### Scope
Add meaningful English JavaDoc for all public methods in all 15 public interface types.

### Current State

| Interface | Class Doc? | Methods | Documented |
|-----------|-----------|---------|------------|
| SuperduperObserver | No | 13 | 6 |
| ConsumerMetadataResolver | No | 4 | 0 |
| MessageIngestRepository | No | 3 | 0 |
| ReactiveMessageIngestRepository | No | 3 | 0 |
| ReactiveWorkerMaintenanceRepository | Yes | 9 | 8 |
| ReactiveWorkerMessageRepository | No | 12 | 5 |
| TopicConfigView | No | 7 | 0 |
| TopicRegistryView | No | 3 | 0 |
| TopicRepositoryFactory | No | 6 | 0 |
| WorkerMaintenanceRepository | Yes | 9 | 8 |
| WorkerMessageRepository | No | 12 | 5 |
| JdbcSqlDialect | No | 17 | 0 |
| R2dbcSqlDialect | No | 17 | 0 |
| MessageHandler | No | 1 | 0 |
| ReactiveMessageHandler | No | 1 | 0 |
| **Totals** | **2/15** | **~117** | **~32** |

~85 public methods need JavaDoc. 13 interfaces need class-level JavaDoc.

### Implementation Steps

#### 1. Add class-level JavaDoc to all 15 interfaces
- Each interface gets a one-to-three sentence description explaining its role, when it is used, and which module consumers interact with it.

#### 2. Add method-level JavaDoc to all undocumented methods (~85 methods)
- Priority order:
  1. **User extension points** (highest visibility): `MessageHandler`, `ReactiveMessageHandler`, `ConsumerMetadataResolver`.
  2. **Repository API contracts**: `MessageIngestRepository`, `ReactiveMessageIngestRepository`, `WorkerMessageRepository`, `ReactiveWorkerMessageRepository`, `WorkerMaintenanceRepository`, `ReactiveWorkerMaintenanceRepository`.
  3. **Topic model**: `TopicConfigView`, `TopicRegistryView`, `TopicRepositoryFactory`.
  4. **SQL dialect SPIs**: `JdbcSqlDialect`, `R2dbcSqlDialect`.
  5. **Observability**: remaining undocumented methods in `SuperduperObserver`.
- Each method gets: summary sentence, `@param` for each parameter, `@return` description, `@throws` if applicable.

#### 3. Review and consistency pass
- Ensure consistent terminology (e.g., "claim" not "lock", "topic" not "queue").
- Ensure English only (no German remnants).
- Run `mvn -q spotless:apply` to format JavaDoc.

### Acceptance Criteria
- All 15 public interfaces have class-level JavaDoc.
- All ~117 public methods have method-level JavaDoc with `@param`, `@return`, and `@throws` where applicable.
- All JavaDoc is in English.
- `mvn -q spotless:apply` produces no diff after the changes.

### Validation
- `mvn -q -DskipTests test-compile` passes.
- Manual review of JavaDoc quality and consistency.

---

## T-005 — Maven Central Preparation

### Scope
Prepare all technical prerequisites to publish the library to Maven Central, including POM metadata, signing, publishing configuration, and a dry-run validation.

### Current State
The parent POM is missing all Maven Central required metadata:
- No `<name>`, `<description>`, `<url>`.
- No `<licenses>`, `<scm>`, `<developers>`.
- No `<distributionManagement>`.
- No `maven-gpg-plugin` (signing).
- No publishing plugin (`central-publishing-maven-plugin` or `nexus-staging-maven-plugin`).

### Implementation Steps

#### 1. Add required POM metadata to parent POM
- `<name>`: `SUPERDUPER`
- `<description>`: Resilient, ordered, database-backed Kafka consumption pattern with pluggable workers.
- `<url>`: GitHub repository URL.
- `<licenses>`: MIT license block.
- `<scm>`: `connection`, `developerConnection`, `url` pointing to the GitHub repo.
- `<developers>`: developer entry with id, name, email.
- `<inceptionYear>`: project start year.

#### 2. Add child module metadata
- Each child POM gets a `<name>` and `<description>` appropriate to its artifact.
- Alternatively, use `<name>${project.artifactId}</name>` in the parent and override only where needed.

#### 3. Configure artifact signing
- Add `maven-gpg-plugin` in a `release` profile in the parent POM.
- Profile activation: `-Prelease` or by property (`-Dgpg.sign=true`).
- Configure the plugin to use `--pinentry-mode loopback` for CI environments.
- In CI (`release.yml`), import the GPG key from GitHub secrets and set the passphrase.

#### 4. Configure Maven Central publishing
- Use `central-publishing-maven-plugin` (the modern Sonatype Central Portal approach) or `nexus-staging-maven-plugin` (legacy OSSRH).
  - Recommendation: `central-publishing-maven-plugin` — newer, simpler, maintained.
- Add `maven-source-plugin` execution to attach source JARs.
- Add `maven-javadoc-plugin` execution to attach JavaDoc JARs.
- Configure `<distributionManagement>` pointing to Central.

#### 5. Configure the release publish CI workflow
- Complete the skeleton `release.yml` from T-001:
  - Import GPG key from secrets.
  - Run `mvn -Prelease deploy` to sign, package, and publish.
  - Use `MAVEN_CENTRAL_TOKEN` / `MAVEN_CENTRAL_USERNAME` from GitHub secrets.

#### 6. Dry-run validation
- Run `mvn -Prelease verify -DskipTests` locally to ensure:
  - Source JARs are attached.
  - JavaDoc JARs are attached.
  - GPG signatures are generated.
  - POM passes Central validation.
- Alternatively, publish to a staging repository and verify, then drop.

#### 7. Exclude example modules from publishing
- Add `<maven.deploy.skip>true</maven.deploy.skip>` to each example module POM.

### Acceptance Criteria
- Parent POM contains all Maven Central required metadata (name, description, url, licenses, scm, developers).
- `maven-gpg-plugin` is configured in a `release` profile.
- `maven-source-plugin` and `maven-javadoc-plugin` attach artifacts in the `release` profile.
- Publishing plugin is configured (Central Portal or OSSRH).
- Example modules are excluded from deploy.
- `mvn -Prelease verify -DskipTests` passes locally, producing signed artifacts with source and JavaDoc JARs.
- `.github/workflows/release.yml` is complete with signing and publishing steps.
- Document the release workflow: build → sign → publish → verify.

### Validation
- `mvn -Prelease verify -DskipTests` passes.
- Inspect `target/` for `.jar`, `-sources.jar`, `-javadoc.jar`, and `.asc` files.
- Dry-run deploy to staging (if OSSRH) or validate with Central Portal.

---

## Global Validation (before release)
- `mvn -q spotless:apply` — no formatting drift.
- `mvn -q -DskipTests test-compile` — all modules compile.
- `mvn -T 1C -q test` — all tests pass.
- `mvn verify` — JaCoCo check passes at ≥80%.
- CI pipeline green on push.
- SonarCloud quality gate passes.
- `mvn -Prelease verify -DskipTests` — signed artifacts produced.
