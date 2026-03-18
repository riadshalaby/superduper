# Plan — 0.6.4-SNAPSHOT

Status: **ready**

Goal: improve Sonar signal quality by excluding test sources, and improve release note quality via release-drafter.

---

## Task T-001 — Exclude test sources from Sonar analysis

### Scope

Add Sonar exclusion properties to the parent POM so that test source files are not analyzed as production code. Document the patterns in project docs.

### Current State

- Parent `pom.xml` has Sonar properties (org, host, project key, coverage paths) but **no exclusion properties**.
- Example modules already set `<sonar.skip>true</sonar.skip>`.
- Sonar runs on `main` only (`ci.yml` sonar job, line 81-167).

### Implementation

#### Phase 1 — Add Sonar exclusions to parent POM

In `pom.xml` `<properties>`, add:

```xml
<sonar.exclusions>**/src/test/**</sonar.exclusions>
```

This tells SonarCloud to exclude all test source files from production-code analysis (issues, code smells, duplication). Coverage data from JaCoCo is unaffected because coverage is reported via `sonar.coverage.jacoco.xmlReportPaths` which already points at the aggregated report.

#### Phase 2 — Document the exclusion patterns

Add a `## Sonar Configuration` section to `docs/ARCHITECTURE.md` documenting:

- Which exclusion properties are set and why.
- That example modules use `sonar.skip=true`.
- How to verify exclusions in SonarCloud (project settings → General → Source File Exclusions).

#### Phase 3 — Validate

- Run `mvn -q -DskipTests test-compile` (compile check).
- Run `mvn -T 1C -q test` (no test regressions).
- Sonar exclusion will be validated on next `main` merge by inspecting the SonarCloud dashboard to confirm test files no longer appear in production code analysis.

### Acceptance Criteria

1. `sonar.exclusions` property is set in parent `pom.xml`.
2. `docs/ARCHITECTURE.md` documents the Sonar exclusion patterns.
3. Build compiles and tests pass.
4. After merge to `main`, SonarCloud dashboard shows zero test files in production code analysis.

---

## Task T-002 — Structured release notes via release-drafter

### Scope

Replace the plain `--generate-notes` output with `release-drafter/release-drafter`, enforce PR labels, add a PR template, curate categories, and modify CI to publish the drafter's output.

### Current State

- CI `release` job creates GitHub Releases with `gh release create --generate-notes --verify-tag` (ci.yml line 318-321).
- `.github/release.yml` has basic categories: Features (`feat`), Bug Fixes (`fix`), Performance (`perf`), Other Changes (`*`); excludes `skip-changelog`.
- No PR template exists.
- No release-drafter configuration exists.

### Implementation

#### Phase 1 — Add release-drafter GitHub Action workflow

Create `.github/workflows/release-drafter.yml`:

```yaml
name: Release Drafter

on:
  push:
    branches:
      - main
  pull_request_target:
    types: [opened, reopened, synchronize]

permissions:
  contents: read
  pull-requests: write

jobs:
  update-release-draft:
    runs-on: ubuntu-latest
    steps:
      - uses: release-drafter/release-drafter@v6
        with:
          config-name: release-drafter.yml
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

#### Phase 2 — Create release-drafter configuration

Create `.github/release-drafter.yml`:

```yaml
name-template: 'v$RESOLVED_VERSION'
tag-template: 'v$RESOLVED_VERSION'
categories:
  - title: '🚀 Features'
    labels:
      - feat
      - feature
      - enhancement
  - title: '🐛 Bug Fixes'
    labels:
      - fix
      - bugfix
      - bug
  - title: '⚡ Performance'
    labels:
      - perf
      - performance
  - title: '📖 Documentation'
    labels:
      - docs
      - documentation
  - title: '💥 Breaking Changes'
    labels:
      - breaking
      - breaking-change
exclude-labels:
  - skip-changelog
  - chore
  - test
  - ci
  - refactor
change-template: '- $TITLE @$AUTHOR (#$NUMBER)'
change-title-escapes: '\<*_&'
version-resolver:
  major:
    labels:
      - breaking
      - breaking-change
  minor:
    labels:
      - feat
      - feature
      - enhancement
  patch:
    labels:
      - fix
      - bugfix
      - bug
      - perf
      - performance
  default: patch
template: |
  ## Changes

  $CHANGES

  ## Contributors

  $CONTRIBUTORS
```

#### Phase 3 — Update `.github/release.yml` categories

Update the existing `.github/release.yml` to align category labels with release-drafter:

```yaml
changelog:
  categories:
    - title: Features
      labels:
        - feat
        - feature
        - enhancement
    - title: Bug Fixes
      labels:
        - fix
        - bugfix
        - bug
    - title: Performance
      labels:
        - perf
        - performance
    - title: Documentation
      labels:
        - docs
        - documentation
    - title: Breaking Changes
      labels:
        - breaking
        - breaking-change
  exclude:
    labels:
      - skip-changelog
      - chore
      - test
      - ci
      - refactor
```

#### Phase 4 — Create PR template

Create `.github/PULL_REQUEST_TEMPLATE.md`:

```markdown
## Summary

<!-- Brief description of the changes -->

## Type of Change

<!-- Check all that apply. Add the matching label to the PR. -->

- [ ] `feat` — New feature
- [ ] `fix` — Bug fix
- [ ] `perf` — Performance improvement
- [ ] `docs` — Documentation only
- [ ] `breaking` — Breaking change
- [ ] `chore` — Maintenance (excluded from release notes)
- [ ] `test` — Test only (excluded from release notes)
- [ ] `ci` — CI/CD only (excluded from release notes)
- [ ] `refactor` — Refactor without user impact (excluded from release notes)

## Release Notes

<!-- User-facing summary for release notes. Leave blank for chore/test/ci/refactor. -->

## Breaking Changes

<!-- Describe any breaking changes and migration steps. Remove section if N/A. -->

## Test Plan

- [ ] Tests pass locally (`mvn -T 1C -q test`)
- [ ] Formatting passes (`mvn -q spotless:check`)
```

#### Phase 5 — Modify CI release job to publish draft

In `ci.yml`, replace the `Create GitHub Release` step (lines 308-321) to publish the existing release-drafter draft instead of using `--generate-notes`:

```yaml
      - name: Create GitHub Release
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          version="${{ needs.tag-version.outputs.version }}"
          tag="v$version"
          if gh release view "$tag" >/dev/null 2>&1; then
            echo "GitHub Release $tag already exists; skipping creation."
            exit 0
          fi

          # Look for an existing draft release created by release-drafter
          draft_tag=$(gh release list --json tagName,isDraft --jq '.[] | select(.isDraft==true) | .tagName' | head -n1)

          if [[ -n "$draft_tag" ]]; then
            # Publish the draft, updating its tag to the actual release tag
            gh release edit "$draft_tag" --tag "$tag" --title "$tag" --draft=false --verify-tag
          else
            # Fallback: create release with auto-generated notes
            gh release create "$tag" \
              --title "$tag" \
              --generate-notes \
              --verify-tag
          fi
```

#### Phase 6 — Document in project docs

Add a `## Release Notes` section to `docs/RELEASE.md` documenting:

- The release-drafter workflow and how it accumulates draft notes.
- PR labeling requirements and the label-to-category mapping.
- The maintainer review step: review the draft on GitHub before cutting a release.
- How CI publishes the draft on tag creation.

### Acceptance Criteria

1. `.github/workflows/release-drafter.yml` workflow exists and runs on `push:main` and `pull_request_target`.
2. `.github/release-drafter.yml` config defines curated categories (Features, Bug Fixes, Performance, Docs, Breaking Changes) and excludes noise labels (chore, test, ci, refactor).
3. `.github/release.yml` categories align with release-drafter labels.
4. `.github/PULL_REQUEST_TEMPLATE.md` includes label checklist, Release Notes section, and Breaking Changes section.
5. CI `release` job publishes the release-drafter draft when available, falls back to `--generate-notes`.
6. `docs/RELEASE.md` documents the release notes workflow.
7. Build compiles and tests pass.

---

## Validation (both tasks)

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
- Manual verification on SonarCloud after `main` merge (T-001).
- Manual verification of release-drafter draft after PR merge to `main` (T-002).
