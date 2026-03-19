# Release Workflow

SUPERDUPER publishes releases from the unified CI workflow in `.github/workflows/ci.yml`.

## Overview

release-please automates version bumps, `CHANGELOG.md` updates, GitHub Releases, and release tags from Conventional Commits merged to `main`. `scripts/ai-start-cycle.sh` bootstraps a new development cycle by creating a branch, resetting the cycle files from templates, committing, and pushing the branch.

## Release Flow

1. Develop feature work on `feature/`, `fix/`, or `chore/` branches and merge it to `main` through reviewed PRs.
2. Use `scripts/ai-pr.sh sync` for feature PRs. It generates the Summary, Breaking Changes, Included Commits, and Test Plan sections from branch commits.
3. On pushes to `main`, `release-please` in `.github/workflows/ci.yml` detects releasable commits and creates or updates a Release PR with version bumps and `CHANGELOG.md` entries.
4. Review and merge the release-please-managed Release PR when the version is ready to cut.
5. Merging the Release PR triggers the automated release path on `main`: release-please creates the tag and GitHub Release, and the `publish` job deploys to Maven Central when the release secrets are present.

## Starting a New Development Cycle

Run:

```bash
scripts/ai-start-cycle.sh <branch-name>
```

The script:
- validates that the branch name starts with `feature/`, `fix/`, or `chore/`
- checks out the latest `main`
- creates and pushes the new branch
- resets `.ai/PLAN.md`, `.ai/REVIEW.md`, `.ai/TASKS.md`, and `ROADMAP.md` from their templates
- removes `.ai/HANDOFF.md` if present
- commits the reset with `chore: start cycle <name>`

After bootstrapping the branch, define the cycle scope in `ROADMAP.md`, then follow the usual plan -> implement -> review workflow.

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
