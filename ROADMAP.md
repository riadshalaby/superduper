# ROADMAP

- why does the Workerservices use a claimedCount Array even if claimCount is always a single long value ?
- When Processing of a message fails The result should be Failure not retry. The message should then be marked as failed with a retry count until max retry count is reached. then the message should be marked as stopped
- Onwershipt check in status update SQL is missing
- Heartbeat window equals heartbeat interval → false orphan detection 
- Reactive consumer silently swallows DB write errors
- `ReactiveOrphanReclaimer` uses `subscribe()` with `fixedDelay` → overlapping runs
- Heartbeat uses `fixedRate` instead of `fixedDelay`
- SuperDuperWorkerReactiveService.schedule() should use a Scheduler with virttual threads 
- At-least-once semantics undocumented 
- When messages are reclaimed from `PROCESSING` → `READY`, `last_updated` is not refreshed
- 