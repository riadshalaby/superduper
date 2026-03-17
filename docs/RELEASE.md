# Release Workflow

SUPERDUPER publishes releases to Maven Central from the GitHub tag workflow in `../.github/workflows/release.yml`.

## Flow

1. Build
   - Merge the release PR to `main`.
   - Wait for the `CI` workflow on `main` to complete successfully.
   - Push a tag that matches `v*`.

2. Sign
   - The release workflow checks out the tagged commit and configures Java and Maven credentials.
   - `MAVEN_GPG_KEY` and `MAVEN_GPG_PASSPHRASE` are passed to the Maven GPG plugin in the `release` profile.
   - The `release` profile attaches `-sources.jar`, `-javadoc.jar`, and `.asc` signatures.

3. Publish
   - The workflow runs `mvn -Prelease -DskipTests deploy`.
   - The Sonatype Central Portal publish step uses the `central` server credentials from `MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_TOKEN`.
   - Example applications and the internal `coverage-report` module are excluded from publishing.

4. Verify
   - The release workflow waits for Central publication to complete.
   - After the run succeeds, verify the new version on Maven Central and in the GitHub release/tag history.

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
