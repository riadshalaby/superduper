# Review

2026-03-09 08:20 UTC

Status: **changes requested**

## Findings

1. Medium — The documented multi-container entrypoint still does not start Prometheus or Grafana. [`examples/run-multi.sh:92`](/Users/riadshalaby/localrepos/superduper/examples/run-multi.sh#L92) builds `selected_services` from Kafka/Postgres/Adminer plus the chosen seeders/workers, but it never includes the `prometheus` or `grafana` services that are defined in [`docker-compose.multi.yml:271`](/Users/riadshalaby/localrepos/superduper/docker-compose.multi.yml#L271). That makes the new observability stack unreachable through the repo’s advertised “real life multi containers” workflow, even though [`docs/EXAMPLES.md:82`](/Users/riadshalaby/localrepos/superduper/docs/EXAMPLES.md#L82) tells users those endpoints will be available and the plan explicitly calls for Prometheus/Grafana-backed multi-container observability. [`.ai/PLAN.md:220`](/Users/riadshalaby/localrepos/superduper/.ai/PLAN.md#L220)

The previous review issues are addressed:

- Reactive cleanup now reports the specific failing operation in `maintenanceFailed`, matching the blocking path. [`ReactiveCleanupService.java:35`](/Users/riadshalaby/localrepos/superduper/worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveCleanupService.java#L35)
- The new repository cleanup APIs and retention properties now carry Javadoc, satisfying the plan checklist. [`WorkerMaintenanceRepository.java:3`](/Users/riadshalaby/localrepos/superduper/repository-api/src/main/java/net/rsworld/superduper/repository/api/WorkerMaintenanceRepository.java#L3) [`ReactiveWorkerMaintenanceRepository.java:5`](/Users/riadshalaby/localrepos/superduper/repository-api/src/main/java/net/rsworld/superduper/repository/api/ReactiveWorkerMaintenanceRepository.java#L5) [`WorkerProperties.java:164`](/Users/riadshalaby/localrepos/superduper/starter-autoselect/src/main/java/net/rsworld/superduper/starter/WorkerProperties.java#L164)
- The retention docs now include the manual archive-before-delete workflow requested by the plan. [`USAGE.md:431`](/Users/riadshalaby/localrepos/superduper/docs/USAGE.md#L431)

## Validation

- Passed: `mvn -q -DskipTests test-compile`
- Passed: `mvn -q -pl starter-autoselect,observability-logging,observability-metrics,worker-blocking,worker-reactive -am -Dtest=AutoSelectConfigurationTest,LoggingSuperduperObserverTest,MetricsSuperduperObserverTest,CleanupServiceTest,ReactiveCleanupServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Passed: `mvn -q -pl repository-jdbc -am -Dtest=JdbcCleanupIntegrationTest,JdbcCleanupMariaDbIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Passed: `mvn -q -pl repository-r2dbc -am -Dtest=R2dbcCleanupIntegrationTest,R2dbcCleanupMariaDbIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Passed: `mvn -q spotless:check`

## Residual Risk

- I did not rerun the full `mvn -T 1C -q test` suite in this review pass.
- I did not start the example applications or bring up Docker Compose, so the example endpoints, Prometheus scrape, and Grafana dashboard remain unverified in this pass.
- [`ROADMAP.md:1`](/Users/riadshalaby/localrepos/superduper/ROADMAP.md#L1) is still scoped to `0.4.x`. [`.ai/PLAN.md:528`](/Users/riadshalaby/localrepos/superduper/.ai/PLAN.md#L528) marks the `0.5.x` roadmap rewrite as an after-merge step, so I am not treating that as a branch finding.
