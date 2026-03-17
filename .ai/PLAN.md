# Plan — 0.6.2-SNAPSHOT

Status: **active**

Goal: unify the CI pipeline so Sonar runs non-blocking on `main`, the release flow lives inside `ci.yml`, and strict execution order prevents accidental releases.

---

## Task Overview

| Task ID | Scope | Dependencies | Estimated Size |
|---------|-------|-------------|----------------|
| T-001 | Unified CI Pipeline Consolidation | none | medium |

---

## T-001 — Unified CI Pipeline Consolidation

### Scope
Consolidate the release workflow into `ci.yml`, make Sonar non-blocking on `main`, and enforce strict job execution order with safety gates. Delete `release.yml` after migration.

### Current State

**`ci.yml`** has two jobs:
- `build` — runs on all pushes and PRs: checkout, Java 25, spotless, compile, test, JaCoCo verify, artifact upload.
- `sonar` — runs on `main` push only, after `build` succeeds. Uses `sonar.qualitygate.wait=true` which **fails the job when the quality gate is red**.

**`release.yml`** has two jobs (triggered on `v*` tag push):
- `verify-main-ci` — queries the GitHub API to verify a successful CI run exists for the tagged commit on `main`.
- `publish` — after `verify-main-ci` succeeds: sets up Java with Maven Central credentials, verifies secrets, runs `mvn -B -Prelease -DskipTests deploy`.

### Problems Being Solved

1. **Sonar blocks the pipeline.** `sonar.qualitygate.wait=true` means a red quality gate fails the `sonar` job and marks the workflow as failed. The intent is visibility, not blocking.
2. **Release logic is duplicated across two files.** The `release.yml` workflow independently re-checks out code, sets up Java, and verifies CI. This should be a job within `ci.yml` gated by the build job.
3. **No enforced sequencing.** The current `release.yml` only verifies that *some* CI run passed for the tagged SHA. Moving release into `ci.yml` makes the dependency chain explicit via `needs`.

### Implementation Steps

#### 1. Make Sonar non-blocking in `ci.yml`

In the `sonar` job:
- Remove `-Dsonar.qualitygate.wait=true` from the `mvn sonar:sonar` command.
- Add a separate step after `sonar:sonar` that checks the quality gate status via the SonarCloud API and logs the result.
- This step should use `continue-on-error: true` so a red gate is visible in the logs but does not fail the workflow.
- Add explicit log messaging: print a clear warning when the quality gate is red, and a success message when it's green.

The resulting Sonar step sequence:
```yaml
- name: Sonar analysis
  if: ${{ env.SONAR_TOKEN != '' }}
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: mvn -B -DskipTests sonar:sonar

- name: Check Sonar quality gate
  if: ${{ env.SONAR_TOKEN != '' }}
  continue-on-error: true
  env:
    SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}
  run: |
    # Query SonarCloud API for quality gate status
    # Log result clearly: PASSED or FAILED (with link to dashboard)
    # Exit 1 on failure so the step shows as orange/warning, but continue-on-error prevents job failure
```

#### 2. Add release jobs to `ci.yml`

Add two new jobs to `ci.yml`:

**Job: `tag-version`**
- Runs only on `main` push events (not PRs, not tags, not other branches).
- Condition: `if: github.event_name == 'push' && github.ref == 'refs/heads/main'`
- `needs: [build]` — only runs after a successful build.
- Checks if the current commit's project version matches a release pattern (no `-SNAPSHOT` suffix).
- If it is a release version, creates and pushes the `v<version>` tag.
- If it is a snapshot version, skips tagging (no-op).
- Outputs a flag (`is_release: true/false`) and the version string for downstream jobs.

**Job: `release`**
- Runs only when `tag-version` outputs `is_release: true`.
- Condition: `if: needs.tag-version.outputs.is_release == 'true'`
- `needs: [build, tag-version]` — strict dependency chain.
- Contains the publishing logic currently in `release.yml` → `publish` job:
  - Setup Java with Maven Central server credentials.
  - Verify release secrets are present (`MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_TOKEN`, `MAVEN_GPG_KEY`, `MAVEN_GPG_PASSPHRASE`).
  - Run `mvn -B -Prelease -DskipTests deploy`.
- Permissions: `contents: read` (tag was already pushed by `tag-version`).

#### 3. Update `tag-version` job to handle tagging safely

The `tag-version` job needs:
- `permissions: contents: write` (to push the tag).
- Extract version from `pom.xml` (use `mvn help:evaluate` or parse the XML).
- Check if version ends with `-SNAPSHOT` → if so, output `is_release=false` and skip.
- Check if tag `v<version>` already exists remotely → if so, output `is_release=false` and skip (idempotent).
- Otherwise, create and push the tag.

#### 4. Enforce safety conditions across all jobs

Ensure no job can accidentally run outside its intended context:
- `build` — runs on all pushes and PRs (unchanged).
- `sonar` — runs only on `main` push after successful `build` (unchanged except non-blocking).
- `tag-version` — runs only on `main` push after successful `build`. Never on PRs. Never on feature branches.
- `release` — runs only when `tag-version` outputs `is_release: true`. Never directly triggered.

Add explicit `if` conditions on every job to prevent accidental execution.

#### 5. Delete `release.yml`

After the new jobs in `ci.yml` are confirmed working:
- Delete `.github/workflows/release.yml`.

#### 6. Update `scripts/ai-release.sh`

The `finalize_release()` function currently pushes a tag that triggers `release.yml`. With the new model:
- Tagging on `main` is now handled by CI (`tag-version` job) when a non-snapshot version is merged.
- `finalize_release()` should still push the tag manually as a safety measure (CI also handles it, making it idempotent).
- Update the inline comment on the `git push origin "$tag"` line to note that CI also tags automatically, but the manual push ensures immediate release even if CI hasn't run yet.
- Update the PR body template in `prepare_release()` to remove the reference to `release.yml` and instead reference the unified `ci.yml` release pipeline.

#### 7. Update `CLAUDE.md`

Update the `## CI Pipeline` section:
- Note that all CI/CD logic lives in `ci.yml` (no separate release workflow).
- Sonar runs on `main` as non-blocking (visible but does not fail the pipeline).
- Release flow: `build` → `tag-version` → `release`, all within `ci.yml`.
- `release.yml` was removed and must not be reintroduced.

### Final `ci.yml` Job Structure

```
ci.yml
├── build          (all pushes + PRs)
│   └── spotless → compile → test → JaCoCo → upload artifacts
├── sonar          (main push only, needs: build)
│   └── sonar:sonar → check quality gate (non-blocking)
├── tag-version    (main push only, needs: build)
│   └── extract version → skip if snapshot → create+push tag
└── release        (main push only, needs: build + tag-version, if is_release)
    └── verify secrets → mvn -Prelease deploy
```

### Acceptance Criteria

1. **Sonar non-blocking**: Sonar analysis runs on `main` push. A red quality gate is logged clearly as a warning but does not fail the workflow. A green gate is logged as success.
2. **Unified workflow**: `ci.yml` contains `build`, `sonar`, `tag-version`, and `release` jobs. No separate `release.yml` exists.
3. **Strict execution order**: `tag-version` requires successful `build`. `release` requires successful `build` and `tag-version` with `is_release == true`. Neither `tag-version` nor `release` runs on PRs or non-main branches.
4. **Tagging**: When a non-snapshot version is merged to `main`, CI automatically creates and pushes the version tag. Snapshot versions are skipped.
5. **Publishing**: Release job publishes to Maven Central only when tagging succeeds. Secrets are verified before publish.
6. **Idempotent tagging**: If the tag already exists (e.g., manual push from `finalize`), the `tag-version` job skips gracefully.
7. **`ai-release.sh` updated**: PR body and comments reflect the unified pipeline. Manual tag push in `finalize` is preserved as a safety measure with updated documentation.
8. **`CLAUDE.md` updated**: CI Pipeline section reflects the new unified model.

### Validation

- Push to a non-main branch → only `build` runs.
- Open a PR to `main` → only `build` runs.
- Push a snapshot version to `main` → `build` + `sonar` + `tag-version` (skips tagging) run. No `release`.
- Push a release version to `main` → `build` + `sonar` + `tag-version` (creates tag) + `release` (publishes) run.
- Sonar quality gate red → warning logged, workflow still green.
- `mvn -q -DskipTests test-compile` passes locally.

---

## Global Validation
- `mvn -q spotless:apply` — no formatting drift.
- `mvn -q -DskipTests test-compile` — all modules compile.
- `mvn -T 1C -q test` — all tests pass.
