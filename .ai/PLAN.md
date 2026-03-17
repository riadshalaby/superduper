# Plan — 0.6.3-SNAPSHOT

Status: **active**

Goal: make the release trigger deterministic by establishing CI as the sole tag creator, removing tag creation from `finalize`, and adding automatic GitHub Release notes generation.

---

## Task Overview

| Task ID | Scope | Dependencies | Estimated Size |
|---------|-------|-------------|----------------|
| T-001 | Release Trigger Consistency + Release Notes | none | medium |

---

## T-001 — Release Trigger Consistency + Release Notes

### Scope
Remove the conflict between manual tagging in `finalize` and automatic tagging in CI. Make CI the single authoritative path for tag creation and publishing. Add automatic GitHub Release creation with release notes generated from commit messages.

### Current State

**Two competing tag creators:**
1. `ci.yml` `tag-version` job — on `main` push, if version is non-snapshot and tag doesn't exist, creates and pushes `v<version>` tag. `release` job then publishes to Maven Central.
2. `ai-release.sh finalize` — after user confirms merge, also tries to push `v<version>` tag (with "already exists" skip logic).

**Race condition:**
- If `finalize` runs before CI completes → manual tag push beats CI → CI sees tag exists → sets `is_release=false` → `release` job **skipped** → Maven Central publish **doesn't happen**.
- If CI runs first → `finalize` sees tag exists → skips → but the user doesn't know whether CI already published or not.

**No GitHub Release:**
- No `gh release create` step exists anywhere.
- No release notes are generated from commits or PR descriptions.

### Target Release Flow

```
1. User runs: scripts/ai-release.sh prepare X.Y.Z
   → bumps version, commits, pushes branch, opens PR to main

2. User merges PR on GitHub

3. CI runs on main push (automatic):
   build → tag-version (creates v<X.Y.Z> tag) → release (publishes to Maven Central + creates GitHub Release with notes)

4. User runs: scripts/ai-release.sh finalize X.Y.Z [NEXT_VERSION]
   → verifies tag exists on origin (created by CI)
   → creates next dev branch, bumps to NEXT_VERSION-SNAPSHOT, resets cycle files
```

### Implementation Steps

#### 1. Remove tag creation from `finalize` in `ai-release.sh`

In `finalize_release()`, replace the tag creation and push logic (lines ~359–374) with a **read-only verification**:

```bash
local tag="v$release"
# CI on main is the authoritative tag creator. Verify the tag exists before proceeding.
if ! git ls-remote --tags origin "refs/tags/$tag" | grep -q "refs/tags/$tag"; then
  die "Tag $tag not found on origin. Wait for CI on main to complete tag-version and release jobs, then re-run finalize."
fi
echo "Verified tag $tag exists on origin (created by CI)."
```

Remove:
- The local `git tag "$tag"` creation.
- The `git push origin "$tag"` push.
- The "safety backstop" comment.

This ensures `finalize` never creates tags — it only verifies CI did its job.

#### 2. Add GitHub Release creation to CI `release` job

In `.github/workflows/ci.yml`, add a step to the `release` job after the Maven Central publish step:

```yaml
- name: Create GitHub Release
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: |
    version="${{ needs.tag-version.outputs.version }}"
    tag="v$version"
    gh release create "$tag" \
      --title "v$version" \
      --generate-notes \
      --verify-tag
```

This uses `gh release create --generate-notes` which auto-generates release notes from:
- Commit messages since the previous tag.
- PR titles and descriptions merged since the previous release.
- Formatted consistently by GitHub's built-in generator.

The `--verify-tag` flag ensures the tag exists before creating the release.

The `release` job needs `contents: write` permission (currently `contents: read`) to create the GitHub Release.

#### 3. Update `release` job permissions

Change the `release` job permissions from:
```yaml
permissions:
  contents: read
```
to:
```yaml
permissions:
  contents: write
```

This is needed for `gh release create` to work.

#### 4. Update `prepare_release()` PR body in `ai-release.sh`

Update the PR body template to reflect the new deterministic flow:

- Remove any language suggesting manual tagging.
- Add a note that after merge, CI will automatically: create the release tag, publish to Maven Central, and create a GitHub Release with auto-generated notes.
- Remove the `## Release Checklist` section entirely from the PR body. The checklist duplicates what CI already enforces and what the CI Status section documents.

#### 5. Update `CLAUDE.md` Release Rules and CI Pipeline sections

**CI Pipeline section** — update the release flow description:
- CI on `main` is the sole creator of release tags and GitHub Releases.
- `finalize` no longer pushes tags; it verifies CI created the tag and then handles post-release setup.

**Release Rules section** — update the two-phase workflow description:
- Phase 1 (prepare): unchanged.
- Phase 2 (finalize): remove mention of "creates/pushes tag". Replace with: "Verifies CI has created and pushed the release tag, then creates branch `feature/vNEXT_VERSION`, bumps to next version, resets cycle files."
- Add a new bullet: "CI automatically creates a GitHub Release with auto-generated release notes after publishing to Maven Central."

**Release Safety section** — add:
- "Never create release tags manually; CI on `main` is the sole tag creator."

#### 6. Add `.github/release.yml` configuration (optional but recommended)

Create `.github/release.yml` (not a workflow — this is GitHub's release notes configuration file) to customize how auto-generated notes are categorized:

```yaml
changelog:
  categories:
    - title: Features
      labels:
        - feat
    - title: Bug Fixes
      labels:
        - fix
    - title: Performance
      labels:
        - perf
    - title: Other Changes
      labels:
        - "*"
  exclude:
    labels:
      - skip-changelog
```

Note: Since this project uses Conventional Commits (not PR labels), the `--generate-notes` flag will use commit subjects directly. The categories above are label-based and will mostly fall through to "Other Changes" unless PR labels are adopted. This is acceptable — the auto-generated notes from commit messages are already useful and consistently formatted.

If the implementer finds that `--generate-notes` output is insufficient without labels, an alternative is to build release notes from `git log` in the CI step:

```bash
# Fallback: generate notes from conventional commits
previous_tag="$(git describe --tags --abbrev=0 HEAD^ 2>/dev/null || echo '')"
if [[ -n "$previous_tag" ]]; then
  notes="$(git log --no-merges --format='- %s' "${previous_tag}..${tag}")"
else
  notes="$(git log --no-merges --format='- %s' "${tag}")"
fi
gh release create "$tag" --title "v$version" --notes "$notes" --verify-tag
```

The implementer should try `--generate-notes` first and only fall back if the output is unsatisfactory.

### Files Changed

| File | Change |
|------|--------|
| `scripts/ai-release.sh` | Remove tag creation from `finalize`; add tag verification; update PR body in `prepare` |
| `.github/workflows/ci.yml` | Add GitHub Release step to `release` job; update `release` permissions to `contents: write` |
| `.github/release.yml` | New file — release notes category configuration (optional) |
| `CLAUDE.md` | Update CI Pipeline, Release Rules, and Release Safety sections |

### Acceptance Criteria

1. **Single tag creator**: `ai-release.sh finalize` no longer creates or pushes tags. It verifies the tag exists on origin (created by CI) and fails with a clear message if the tag is missing.
2. **CI creates GitHub Release**: After successful Maven Central publish, the `release` job creates a GitHub Release using `gh release create` with auto-generated notes from commit history.
3. **Release notes quality**: Generated release notes include commit messages since the previous tag, formatted consistently. PR descriptions are included where available.
4. **Deterministic flow**: The release sequence is always: merge PR → CI builds → CI tags → CI publishes → CI creates release → user runs `finalize` for next dev cycle. No alternative paths.
5. **`finalize` resilience**: If the user runs `finalize` before CI has tagged, the script fails with: "Tag v<version> not found on origin. Wait for CI on main to complete tag-version and release jobs, then re-run finalize."
6. **PR body updated**: `prepare` generates a PR body that clearly describes the automated post-merge flow.
7. **Documentation updated**: `CLAUDE.md` reflects the new release model — CI is the sole tag creator, `finalize` is read-only on tags.

### Validation

- Review `ai-release.sh` diff: no `git tag` or `git push origin "$tag"` in `finalize_release()`.
- Review `ci.yml` diff: `release` job has `contents: write` and a `gh release create` step.
- `mvn -q -DskipTests test-compile` passes locally.
- Manually trace through the flow: merge → CI tags → CI publishes → CI creates release → `finalize` verifies and proceeds.

---

## Global Validation
- `mvn -q spotless:apply` — no formatting drift.
- `mvn -q -DskipTests test-compile` — all modules compile.
- `mvn -T 1C -q test` — all tests pass.
