# Review: T-008 — 1.0.0 stable release promotion

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `4836e166d7841eed46ebf9442da16a95970ab80a`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| `release-please-config.json` includes `release-as: 1.0.0` | ✅ | `"release-as": "1.0.0"` on line 6 |
| `README.md` reflects 1.0.0 stable status | ✅ | Line 9: "the next release-please cut is pinned to **`1.0.0`**" — no pre-release or beta language |
| `docs/USAGE.md` reflects 1.0.0 stable status | ✅ | Line 9: "Use the stable `1.0.0` artifacts for the first general-availability release line"; Maven coordinates show `<version>1.0.0</version>` |
| All previous tasks T-001 through T-007 are `done` | ✅ | Confirmed in TASKS.md |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## CLAUDE.md Compliance

| Rule | Status | Notes |
|---|---|---|
| Release version managed via release-please, not manual POM bumps | ✅ | Uses `"release-as": "1.0.0"` in `release-please-config.json` — no POM version changes |
| No `release.yml` or `scripts/ai-release.sh` reintroduced | ✅ | Only files touched: `release-please-config.json`, `README.md`, `docs/USAGE.md` |
| English for code comments, log/output messages, README | ✅ | All documentation in English |

## Findings

No findings. All acceptance criteria satisfied. Implementation is minimal and correct:
- `release-please-config.json`: `"release-as": "1.0.0"` added to the root package block — this is the exact mechanism release-please uses to pin the next Release PR to a specific version.
- `README.md`: release status clearly states the 1.0.0 stable line without any pre-release qualifier; no stale snapshot references remain.
- `docs/USAGE.md`: stable Maven coordinates with explicit `1.0.0` version in all dependency snippets; opening prose confirms this is the first GA release.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are satisfied. `release-please-config.json` is correctly configured with `release-as: 1.0.0`. `README.md` and `docs/USAGE.md` both present the project as a stable GA release with concrete 1.0.0 Maven coordinates — no pre-release or beta language remains. The implementation makes no manual POM version changes, fully respecting the CLAUDE.md release rules. T-001 through T-007 are all `done`, satisfying the dependency requirement.
