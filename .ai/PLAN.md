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

## Task T-003 — Release notes from PR body sections

### Scope

Make the generated release notes consume the PR `## Release Notes` section directly instead of relying on PR titles, so the final draft contains user-written summaries.

### Current State

- release-drafter `change-template` uses `$TITLE`: `'- $TITLE @$AUTHOR (#$NUMBER)'`.
- PR template has a `## Release Notes` section where contributors write user-facing summaries.
- The content of that section is **not used** in the generated draft — only the PR title appears.
- CI `Create GitHub Release` step publishes the draft or falls back to `--generate-notes`.

### Design Decision

**Chosen: Option B — Section extraction script.**

A script in the CI release step extracts the `## Release Notes` section from each merged PR body (via `gh api`), groups entries by release-drafter category labels, and composes curated notes. release-drafter still handles categorization, version resolution, and draft lifecycle. The script enriches the draft body before publishing.

Rejected: Option A (use `$BODY` in change-template) — too noisy; the full PR body includes checkboxes, test plan, HTML comments, requiring heavy manual editing on every release.

### Implementation

#### Phase 1 — Create the extraction script

Create `scripts/compose-release-notes.sh`:

- Input: a git tag or version string identifying the release boundary.
- The script calls `gh api` to list merged PRs to `main` since the previous release tag.
- For each PR, it extracts the content between `## Release Notes` and the next `##` heading (or end of body).
- It strips HTML comments (`<!-- ... -->`).
- If the extracted content is empty or whitespace-only, the PR is skipped (noise labels like `chore` won't have content).
- Entries are grouped by the PR's label into categories matching release-drafter config (Features, Bug Fixes, Performance, Documentation, Breaking Changes).
- PRs with no recognized category label go into an "Other Changes" fallback category.
- Output: a Markdown string suitable for use as a GitHub Release body.

Script outline (~30-40 lines bash):

```bash
#!/usr/bin/env bash
set -euo pipefail

# Determine the previous release tag
CURRENT_TAG="${1:?Usage: compose-release-notes.sh <current-tag>}"
PREV_TAG="$(gh release list --json tagName,isDraft --jq '[.[] | select(.isDraft==false)] | .[0].tagName // empty')"

if [[ -z "$PREV_TAG" ]]; then
  echo "No previous release found; including all merged PRs."
  PREV_TAG=""
fi

# Category label -> heading mapping
declare -A CATEGORY_HEADINGS=(
  [feat]="Features" [feature]="Features" [enhancement]="Features"
  [fix]="Bug Fixes" [bugfix]="Bug Fixes" [bug]="Bug Fixes"
  [perf]="Performance" [performance]="Performance"
  [docs]="Documentation" [documentation]="Documentation"
  [breaking]="Breaking Changes" [breaking-change]="Breaking Changes"
)

# Noise labels to skip entirely
NOISE_LABELS="chore test ci refactor skip-changelog"

# Fetch merged PRs since previous tag
QUERY=".[] | {number, title, body, labels: [.labels[].name]}"
if [[ -n "$PREV_TAG" ]]; then
  MERGE_DATE="$(gh release view "$PREV_TAG" --json publishedAt --jq '.publishedAt')"
  PRS="$(gh pr list --base main --state merged --search "merged:>=$MERGE_DATE" --limit 200 --json number,title,body,labels --jq "$QUERY")"
else
  PRS="$(gh pr list --base main --state merged --limit 200 --json number,title,body,labels --jq "$QUERY")"
fi

# For each PR: extract ## Release Notes section, determine category, accumulate
# ... (sed/awk extraction of section between "## Release Notes" and next "##")
# ... (strip HTML comments)
# ... (group by category heading)
# ... (output grouped Markdown)
```

The full implementation details are left to the implementer. The above is the algorithmic skeleton.

#### Phase 2 — Integrate into CI release step

In `ci.yml`, modify the `Create GitHub Release` step to call the script before publishing:

```yaml
      - name: Checkout  # ensure scripts/ is available
        uses: actions/checkout@v4

      # ... existing steps ...

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

          # Compose release notes from PR body sections
          notes="$(bash scripts/compose-release-notes.sh "$tag" 2>/dev/null || true)"

          if [[ -n "$notes" ]]; then
            gh release create "$tag" \
              --title "$tag" \
              --notes "$notes" \
              --verify-tag
          else
            # Fallback: look for release-drafter draft
            draft_tag="$(gh release list --json tagName,isDraft --jq '.[] | select(.isDraft==true) | .tagName' | head -n1)"
            if [[ -n "$draft_tag" ]]; then
              gh release edit "$draft_tag" --tag "$tag" --title "$tag" --draft=false --verify-tag
            else
              gh release create "$tag" --title "$tag" --generate-notes --verify-tag
            fi
          fi
```

The fallback chain is: composed notes → release-drafter draft → `--generate-notes`.

#### Phase 3 — Update documentation

Update `docs/RELEASE.md` to document:

- That release notes are composed from PR `## Release Notes` sections, not PR titles.
- How the extraction script works and where it lives.
- That PRs without content in `## Release Notes` are silently omitted.
- The three-tier fallback: composed notes → release-drafter draft → `--generate-notes`.

### Acceptance Criteria

1. `scripts/compose-release-notes.sh` exists and is executable.
2. The script extracts content from the `## Release Notes` section of merged PR bodies.
3. The script groups entries by category labels matching release-drafter config.
4. PRs with empty release notes or noise labels are omitted.
5. CI `Create GitHub Release` step calls the script and uses its output as release body.
6. Fallback chain preserved: composed notes → release-drafter draft → `--generate-notes`.
7. `docs/RELEASE.md` updated with the new workflow.
8. Build compiles and tests pass.

---

## Task T-004 — Skip Maven Central publication for selected versions

### Scope

Add a supported `--skip-central` flag to `ai-release.sh prepare` so that selected versions can be released (tagged, GitHub Release created) without publishing to Maven Central.

### Current State

- Parent `pom.xml` already defines `<central.skipPublishing>false</central.skipPublishing>` (line 98).
- The `central-publishing-maven-plugin` in the `release` profile reads `${central.skipPublishing}` via its `<skipPublishing>` config.
- CI `release` job always runs `mvn -B -Prelease -DskipTests deploy`.
- `ai-release.sh prepare` does not have a `--skip-central` flag.
- There is no documented way to skip Maven Central for a specific version.

### Design Decision

**Chosen: Option A — POM property flip via release script flag.**

The `central.skipPublishing` property already exists. The script flips it to `true` in the POM before committing. The skip is visible in the release PR diff. `finalize` resets the POM to the next snapshot, so no cleanup needed.

Rejected: Option B (CI-level variable/label) — less visible in code review, persists across releases. Option C (version pattern) — too rigid for "selected versions."

### Implementation

#### Phase 1 — Add `--skip-central` flag to `ai-release.sh prepare`

Modify `scripts/ai-release.sh`:

1. In `usage()`, add the `--skip-central` option to the prepare usage line:
   ```
   scripts/ai-release.sh prepare <X.Y.Z> [--skip-central]
   ```

2. In `prepare_release()`, parse `--skip-central` from the remaining args:
   ```bash
   local skip_central="false"
   while [[ $# -gt 0 ]]; do
     case "$1" in
       --skip-central) skip_central="true" ;;
       *) die "Unknown option for prepare: $1" ;;
     esac
     shift
   done
   ```

3. After `mvn versions:set`, if `skip_central` is `true`, flip the property in the parent POM:
   ```bash
   if [[ "$skip_central" == "true" ]]; then
     sed -i.bak 's|<central.skipPublishing>false</central.skipPublishing>|<central.skipPublishing>true</central.skipPublishing>|' pom.xml
     rm -f pom.xml.bak
     echo "Maven Central publishing will be skipped for this release."
   fi
   ```

4. The rest of the flow (compile, test, commit, push, PR) proceeds unchanged. The POM change is included in the release commit and visible in the PR diff.

#### Phase 2 — CI awareness (no CI changes needed)

- CI runs `mvn -B -Prelease -DskipTests deploy`.
- The `central-publishing-maven-plugin` reads `${central.skipPublishing}`.
- When `true`, the plugin skips the Central publish. The deploy phase still runs but is a no-op for Central.
- Tag creation and GitHub Release creation proceed as normal.
- No CI workflow changes required.

#### Phase 3 — Update documentation

Update `docs/RELEASE.md` to document:

- The `--skip-central` flag and when to use it (internal-only releases, test releases, versions that should not be on Maven Central).
- That the flag flips `central.skipPublishing` in the POM, which is visible in the release PR.
- That tag and GitHub Release are still created normally.
- That `finalize` resets the POM, so the skip does not carry over to subsequent versions.

Update the PR body template in `ai-release.sh` to note whether Central publishing is skipped.

#### Phase 4 — Validate

- Run `mvn -q -DskipTests test-compile` (compile check).
- Run `mvn -T 1C -q test` (no test regressions).
- Manual test: run `scripts/ai-release.sh prepare` with `--skip-central` on a test branch, verify the POM diff shows `central.skipPublishing` as `true`.

### Acceptance Criteria

1. `ai-release.sh prepare` accepts `--skip-central` flag.
2. When `--skip-central` is passed, `central.skipPublishing` is set to `true` in parent `pom.xml` before the release commit.
3. When `--skip-central` is NOT passed, behavior is unchanged (`central.skipPublishing` remains `false`).
4. The skip is visible in the release PR diff.
5. No CI workflow changes are needed (existing `central-publishing-maven-plugin` config handles it).
6. `docs/RELEASE.md` documents the `--skip-central` flag.
7. PR body from `ai-release.sh` indicates whether Central publishing is skipped.
8. Build compiles and tests pass.

---

## Validation (all tasks)

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
- Manual verification on SonarCloud after `main` merge (T-001).
- Manual verification of release-drafter draft after PR merge to `main` (T-002).
- Manual verification of composed release notes in GitHub Release (T-003).
- Manual test of `--skip-central` flag with `ai-release.sh prepare` (T-004).
