# ROADMAP

Goal: make `v0.6.1` release-ready with measurable code quality, sufficient test coverage, documented public interfaces, and a prepared Maven Central publishing pipeline.

## Priority 1: Code Quality with Sonar

Objective: establish static analysis and enforceable quality gates with Sonar.

- Integrate Sonar analysis into the standard build (local + CI).
- Enable relevant Sonar rules for bugs, vulnerabilities, and code smells.
- Define quality gates and enforce them as merge prerequisites.
- Prioritize and fix existing critical findings.

## Priority 2: Test Coverage at Least 80%

Objective: reach and maintain reliable test coverage of at least 80%.

- Enable consistent coverage measurement in the build.
- Add missing tests in core modules until at least 80% is reached.
- Configure the coverage threshold as a build gate.
- Publish coverage reports as CI artifacts.

## Priority 3: Public Interface Documentation (English)

Objective: provide meaningful English JavaDoc for all public methods in interface types.

- Inventory all `public` methods in interfaces.
- Add clear, meaningful English JavaDoc for each method.
- Review comments for consistency and usefulness.
- Add documentation checks to the review process.

## Priority 4: Maven Central Preparation

Objective: prepare the technical and organizational prerequisites to publish the library cleanly to Maven Central.

- Finalize group ID, artifact ID, and versioning approach.
- Complete POM metadata (name, description, license, SCM, developers).
- Prepare and test signing and publishing configuration.
- Document the release workflow: build, sign, publish, verify.
- Run a dry run and close remaining blockers before final publish.

## Priority 5: CI Pipeline with GitHub Actions (GitHub-Hosted Runners)

Objective: establish an automated CI pipeline on GitHub-hosted runners for build, test, and quality checks.

- Add a GitHub Actions workflow for `push` and `pull_request`.
- Run at least: compile, tests, coverage report generation, and Sonar analysis.
- Configure workflow caching for Maven dependencies to reduce CI runtime.
- Publish test and coverage artifacts for troubleshooting.
- Define the CI workflow as the default quality gate before release preparation.

## Done Criteria for v0.6.1

- Sonar runs in CI and quality gates are active.
- Test coverage is at least 80%.
- All public interface methods have meaningful English JavaDoc.
- Maven Central publishing is technically prepared and validated via dry run.
- GitHub Actions CI runs successfully on GitHub-hosted runners for `push` and `pull_request`.
