# Release Workflow

SUPERDUPER publishes releases from the unified CI workflow in `.github/workflows/ci.yml`.

## Flow

1. Prepare
   - Run `scripts/ai-release.sh prepare X.Y.Z` on the release branch.
   - Review and merge the generated PR to `main`.

2. Build and tag
   - Wait for the `CI` workflow on `main` to complete successfully.
   - The `tag-version` job creates `vX.Y.Z` when the merged `main` version is non-snapshot.

3. Sign and publish
   - The `release` job checks out `main` and configures Java and Maven credentials.
   - `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE` are passed to the Maven GPG plugin in the `release` profile.
   - The `release` profile attaches `-sources.jar`, `-javadoc.jar`, and `.asc` signatures.
   - CI runs `mvn -B -Prelease -DskipTests deploy`.
   - The Sonatype Central Portal publish step uses the `central` server credentials from `MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_TOKEN`.
   - Example applications and the internal `coverage-report` module are excluded from publishing.

4. Finalize
   - After CI succeeds and the release tag exists on origin, run `scripts/ai-release.sh finalize X.Y.Z [NEXT_VERSION]`.
   - The finalize step verifies the CI-created tag, creates the next development branch, bumps the next snapshot version, and resets the cycle files.

## Release Notes

- `.github/workflows/release-drafter.yml` keeps a draft release updated on `main` pushes and PR activity using `.github/release-drafter.yml`.
- PRs should carry one of the release labels used by the drafter configuration: `feat`, `feature`, `enhancement`, `fix`, `bugfix`, `bug`, `perf`, `performance`, `docs`, `documentation`, `breaking`, or `breaking-change`.
- Noise labels `chore`, `test`, `ci`, `refactor`, and `skip-changelog` are excluded from the curated draft release notes.
- `.github/PULL_REQUEST_TEMPLATE.md` prompts contributors to add the matching label and provide a user-facing release note summary when the change should appear in release notes.
- Before cutting a release, review the draft release on GitHub to confirm the categories, summaries, and breaking-change notes are accurate.
- When the release tag is created on `main`, the CI `release` job publishes the draft release. If no draft exists, CI falls back to `gh release create --generate-notes`.

## Required GitHub Secrets

- `MAVEN_CENTRAL_USERNAME`
- `MAVEN_CENTRAL_TOKEN`
- `MAVEN_GPG_KEY`
- `MAVEN_GPG_PASSPHRASE`

## Local Dry Run

Use the release profile locally to verify packaging before creating a tag:

```bash
mvn -Prelease -DskipTests verify
```

If signing credentials are not available locally, you can still validate the release packaging with:

```bash
mvn -Prelease -DskipTests -Dgpg.skip=true verify
```
