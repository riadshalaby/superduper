# ROADMAP

Goal: make the release process as simple as possible.

## Priority 1: Release Trigger Consistency

Objective: remove the conflict between manual tagging in `scripts/ai-release.sh finalize` and automatic tagging/release execution in `.github/workflows/ci.yml`.

- Ensure there is only one authoritative path that creates the release tag.
- Prevent the `release` job on `main` from being skipped just because `finalize` already pushed the tag.
- Keep the release flow deterministic: merge release PR -> `main` CI decides tagging/publishing -> post-release snapshot branch creation.
- Update documentation to reflect the new release flow.
- Add release notes generation to the release process.
- Ensure release notes are generated automatically from commit messages and PR descriptions.
- Ensure release notes are formatted consistently and include relevant information for users and developers.



