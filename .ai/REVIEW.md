# Review — T-005: Maven Central Preparation

- **Verdict:** PASS
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-17T08:50Z
- **Commits reviewed:** 7a14e14 — `build: prepare Maven Central publishing`; 34b693e — `docs: apply MIT license and streamline README`; d0412ad — `build: correct RSWorld organization URL`

---

## Findings (ordered by severity)

### INFO — `doclint: none` disables Javadoc linting in the release profile

**File:** `pom.xml` (maven-javadoc-plugin, `<doclint>none</doclint>`)

Javadoc linting is disabled in the `release` profile. Given the thorough T-004 work the Javadoc is currently clean, but future additions won't be validated at build time. Maven Central does not require `doclint` to pass — it only requires a `-javadoc.jar` to be present — so this does not block publishing.

**No action required.**

---

### INFO — `schema-liquibase` has no deploy-skip and will be published to Central

**File:** `schema-liquibase/pom.xml`

`schema-liquibase` does not set `maven.deploy.skip` or `central.skipPublishing`, so it will be published as part of a release. The plan only specifies excluding example modules. Publishing `schema-liquibase` is the correct behaviour — it contains the Liquibase changelogs that library consumers need to bootstrap the database schema.

**No action required; intentional.**

---

## Plan Compliance Checklist

| Plan step | Expected | Actual | Status |
|-----------|----------|--------|--------|
| `<name>` in parent POM | `SUPERDUPER` | `SUPERDUPER` | PASS |
| `<description>` in parent POM | Resilient, ordered, database-backed... | Matches plan | PASS |
| `<url>` in parent POM | GitHub repo URL | `https://github.com/riadshalaby/superduper` | PASS |
| `<inceptionYear>` | Present | `2026` | PASS |
| `<licenses>` — MIT | Present | MIT block with OSI URL | PASS |
| `<scm>` — connection, developerConnection, url | Present | All three present | PASS |
| `<developers>` — id, name, email | Present | `riadshalaby` / Riad Shalaby / riad@rsworld.eu | PASS |
| `<organization>` | Present | RSWorld / `https://rsworld.eu` | PASS |
| Child module `<name>` + `<description>` | All 12 library modules | All 12 present and accurate | PASS |
| `maven-source-plugin` in `release` profile | `jar-no-fork` | Present | PASS |
| `maven-javadoc-plugin` in `release` profile | `jar` goal | Present | PASS |
| `maven-gpg-plugin` in `release` profile | Present, CI-compatible | `signer: bc` with `MAVEN_GPG_KEY` / `MAVEN_GPG_PASSPHRASE` env vars | PASS |
| `gpg.skip` override for dry-run | Present | `<skip>${gpg.skip}</skip>` | PASS |
| `central-publishing-maven-plugin` in `release` profile | Central Portal plugin | `0.9.0`, `autoPublish: true`, `waitUntil: published` | PASS |
| `<distributionManagement>` | Points to Central | `https://central.sonatype.com/api/v1/publisher` | PASS |
| Example modules excluded from deploy | `maven.deploy.skip=true` + `central.skipPublishing=true` | Set in all 5 example POMs | PASS |
| `coverage-report` excluded from deploy | Same | `maven.deploy.skip=true` + `central.skipPublishing=true` | PASS |
| `release.yml` — GPG secrets wired | `MAVEN_GPG_KEY`, `MAVEN_GPG_PASSPHRASE` | Present as env vars | PASS |
| `release.yml` — Central credentials wired | `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_TOKEN` | Present as env vars; `server-id: central` in setup-java | PASS |
| `release.yml` — publish command | `mvn -Prelease -DskipTests deploy` | `mvn -B -Prelease -DskipTests deploy` | PASS |
| `release.yml` — secrets guard step | Present | `test -n "$VAR"` for all 4 secrets | PASS |
| Dry-run `mvn -Prelease -DskipTests -Dgpg.skip=true verify` | PASS | PASS; source + Javadoc JARs verified in target | PASS |
| `LICENSE` file | MIT | Full MIT text, copyright 2026 Riad Shalaby | PASS |
| `docs/RELEASE.md` | Release workflow documented | Present; covers flow, secrets, local dry-run | PASS |

---

## Quality Assessment

**POM metadata:** All Maven Central required fields are present and correctly formatted. The `<scm>` block includes both `connection` (anonymous git) and `developerConnection` (SSH), which is the standard Maven Central pattern.

**GPG configuration:** Using `signer: bc` (Bouncy Castle) with key and passphrase read from environment variables is the modern, pinentry-free approach for CI — superior to the plan's `--pinentry-mode loopback` suggestion. `bestPractices: true` enables additional safety checks.

**Central plugin configuration:** `autoPublish: true` combined with `waitUntil: published` means the CI step will block until Maven Central confirms full publication, making the release workflow self-contained. `checksums: required` enforces checksum validation. Solid configuration.

**Secrets guard step:** The explicit `test -n "$VAR"` checks for all four secrets before running `deploy` provide a clear, fast failure if secrets are not configured — avoids a confusing mid-publish failure.

**Deploy exclusion:** Both `maven.deploy.skip` and `central.skipPublishing` are set on excluded modules. Belt-and-suspenders: the first prevents the traditional deploy mechanism, the second prevents the Central plugin from picking up the artifact even if the profile is active.

**`docs/RELEASE.md`:** Concise four-step description covering build, sign, publish, and verify phases plus required secrets and local dry-run commands.

**`LICENSE`:** Standard MIT text, correct copyright holder and year.

---

## Required Fixes

None. All acceptance criteria are satisfied.

---

## Summary

All Maven Central prerequisites are in place: required POM metadata on parent and all 12 library modules, a `release` profile with source/Javadoc JARs, Bouncy Castle GPG signing, and the Central Portal publishing plugin. Examples and `coverage-report` are doubly excluded from deploy. The release workflow is complete end-to-end with a secrets guard and a CI gate. A `LICENSE` file and `docs/RELEASE.md` complete the deliverables. Verdict **PASS**.
