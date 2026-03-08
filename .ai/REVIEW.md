# Review

2026-03-08 UTC

Status: **changes requested**

## Findings

1. Medium — The new observability/runtime-diagnostics docs still miss the specific example log output with `correlation_id` and `message_type` metadata fields required by `.ai/PLAN.md`. The plan calls this out explicitly in `.ai/PLAN.md:309`-`.ai/PLAN.md:315`, but the only example log block in `docs/USAGE.md:257`-`docs/USAGE.md:262` shows batch and consumer timing lines without either metadata field. The text at `docs/USAGE.md:278` only recommends including those fields in application logs; it does not provide the documented example the plan asked for.

## Notes

- The redrive docs are present in `docs/USAGE.md:153`-`docs/USAGE.md:182` and the README mention is present at `README.md:194`-`README.md:200`.
- The operational monitoring section is largely in place in `docs/USAGE.md:218`-`docs/USAGE.md:307`, including metric tables, log-line tables, queue/backlog guidance, missing-heartbeat alerting, and orphan-reclaim monitoring guidance.

## Validation

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
