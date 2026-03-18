# ROADMAP

Goal: define and deliver the `0.6.4-SNAPSHOT` scope.

## Priority 1

Objective: exclude test sources from Sonar analysis to improve signal quality.

- Configure Sonar to ignore all test files and test-only directories.
- Validate Sonar reports after the change to confirm only production code is analyzed.
- Document the Sonar exclusion patterns in project docs for maintainability.

## Priority 2

Objective: improve release note quality and make curated notes consistently visible in GitHub Releases.

- Replace plain `--generate-notes` output with a structured release workflow via `release-drafter/release-drafter`.
- Enforce PR labels and a mandatory `Release Notes` section in the PR template (user-facing change, breaking change, migration notes).
- Make the generated release notes consume the PR `Release Notes` section directly so the final draft is not driven mostly by PR titles.
- Add a supported method to exclude selected versions from official Maven release publication when a version should not be pushed to Central.
- Maintain curated categories in `.github/release.yml` (Features, Fixes, Performance, Docs, Breaking Changes) and exclude noise (`chore`, `test`, `ci`, `refactor` without user impact).
- Publish the generated draft as the final GitHub Release notes on tag creation, with a short manual maintainer review before publish.
