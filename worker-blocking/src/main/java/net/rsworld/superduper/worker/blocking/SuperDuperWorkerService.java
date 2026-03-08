package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@SuppressWarnings("java:S107")
public class SuperDuperWorkerService {
    private static final String MODE_BLOCKING = "blocking";
    private static final Logger log = LoggerFactory.getLogger(SuperDuperWorkerService.class);

    private final WorkerMessageRepository messageRepository;
    private final TransactionTemplate tx;
    private final LockingTaskExecutor lockExec;
    private final MessageHandler handler;
    private final SuperduperObserver observer;
    private final String workerId;
    private final int batch;
    private final int maxRetries;
    private final String claimLockName;
    private final Duration lockAtMostFor;
    private final Duration lockAtLeastFor;

    public SuperDuperWorkerService(
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            int batch,
            int maxRetries,
            String claimLockName,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        this(
                messageRepository,
                txm,
                lockExec,
                handler,
                observer,
                batch,
                maxRetries,
                claimLockName,
                lockAtMostForMs,
                lockAtLeastForMs,
                ManagementFactory.getRuntimeMXBean().getName());
    }

    SuperDuperWorkerService(
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            int batch,
            int maxRetries,
            String claimLockName,
            long lockAtMostForMs,
            long lockAtLeastForMs,
            String workerId) {
        this.messageRepository = messageRepository;
        this.tx = new TransactionTemplate(txm);
        this.lockExec = lockExec;
        this.handler = handler;
        this.observer = observer;
        this.batch = batch;
        this.maxRetries = maxRetries;
        this.claimLockName = claimLockName;
        this.lockAtMostFor = Duration.ofMillis(lockAtMostForMs);
        this.lockAtLeastFor = Duration.ofMillis(lockAtLeastForMs);
        this.workerId = workerId;
    }

    public SuperDuperWorkerService(
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            int batch,
            int maxRetries) {
        this(
                messageRepository,
                txm,
                lockExec,
                handler,
                observer,
                batch,
                maxRetries,
                "superduper-claim-batch",
                15000,
                2000);
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.claim-interval-ms:5000}",
            initialDelayString = "${superduper.worker.claim-initial-delay-ms:3000}")
    public void schedule() {
        AtomicLong claimedCount = new AtomicLong(0L);
        long claimStarted = System.nanoTime();
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> claimedCount.set(claimBatch()),
                    new LockConfiguration(Instant.now(), claimLockName, lockAtMostFor, lockAtLeastFor));
            observer.workerClaimed(
                    new WorkerObservation(MODE_BLOCKING, workerId, null, null, batch, elapsedMs(claimStarted)),
                    Math.toIntExact(claimedCount.get()));
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation(MODE_BLOCKING, workerId, null, null, batch, elapsedMs(claimStarted)), e);
            return;
        }
        if (claimedCount.get() > 0) {
            BatchOutcome outcome = process();
            observer.workerBatchCompleted(
                    new WorkerObservation(
                            MODE_BLOCKING, workerId, null, null, outcome.batchSize(), outcome.durationMs()),
                    outcome.processed(),
                    outcome.failed(),
                    outcome.stopped());
        }
    }

    long claimBatch() {
        Long claimed = tx.execute(st -> messageRepository.claimBatch(workerId, batch, maxRetries));
        return claimed == null ? 0L : claimed;
    }

    BatchOutcome process() {
        long started = System.nanoTime();
        var rows = messageRepository.fetchClaimedForWorker(workerId);
        BatchOutcome outcome = new BatchOutcome(rows.size());
        for (var keyGroup : groupRowsByMessageKey(rows).values()) {
            for (int index = 0; index < keyGroup.size(); index++) {
                ClaimedMessage row = keyGroup.get(index);
                ProcessResult result = processOne(row);
                outcome.record(result.outcome());
                if (result.continueKey()) {
                    continue;
                }
                releaseRemaining(keyGroup, index + 1);
                break;
            }
        }
        outcome.complete(elapsedMs(started));
        return outcome;
    }

    private Map<String, List<ClaimedMessage>> groupRowsByMessageKey(List<ClaimedMessage> rows) {
        Map<String, List<ClaimedMessage>> groupedRows = new LinkedHashMap<>();
        for (ClaimedMessage row : rows) {
            groupedRows
                    .computeIfAbsent(row.messageKey(), ignored -> new ArrayList<>())
                    .add(row);
        }
        return groupedRows;
    }

    private ProcessResult processOne(ClaimedMessage row) {
        long started = System.nanoTime();
        ProcessingResult result = ProcessingResult.FAILURE;
        Throwable failureCause = null;
        try {
            result = handler.handle(new MessageRow(
                    row.id(),
                    row.messageId(),
                    row.messageKey(),
                    row.content(),
                    "PROCESSING",
                    row.retryCount(),
                    row.containerId(),
                    row.correlationId(),
                    row.messageType()));
        } catch (RuntimeException | MessageHandlingException e) {
            failureCause = e;
            observer.workerFailed(
                    new WorkerObservation(
                            MODE_BLOCKING, workerId, row.id(), row.retryCount(), batch, elapsedMs(started)),
                    e);
        }

        int retry = row.retryCount() == null ? 0 : row.retryCount();
        if (result == ProcessingResult.SUCCESS) {
            boolean updated = messageRepository.markProcessed(row.id(), workerId);
            if (!updated) {
                warnOwnershipLost(row.id(), "markProcessed");
            } else {
                observer.workerProcessed(
                        new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
                return new ProcessResult(true, MessageOutcome.PROCESSED);
            }
            return new ProcessResult(true, MessageOutcome.NONE);
        }

        if (failureCause == null) {
            observer.workerFailed(
                    new WorkerObservation(
                            MODE_BLOCKING, workerId, row.id(), row.retryCount(), batch, elapsedMs(started)),
                    new IllegalStateException("Message handler returned FAILURE"));
        }
        retry++;
        if (retry < maxRetries) {
            boolean updated = messageRepository.markFailed(row.id(), retry, workerId);
            if (!updated) {
                warnOwnershipLost(row.id(), "markFailed");
            } else {
                observer.workerRetried(
                        new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
                return new ProcessResult(false, MessageOutcome.FAILED);
            }
        } else {
            boolean updated = messageRepository.markStopped(row.id(), retry, workerId);
            if (!updated) {
                warnOwnershipLost(row.id(), "markStopped");
            } else {
                observer.workerStopped(
                        new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
                return new ProcessResult(false, MessageOutcome.STOPPED);
            }
        }
        return new ProcessResult(false, MessageOutcome.NONE);
    }

    private void releaseRemaining(List<ClaimedMessage> rows, int fromIndex) {
        if (fromIndex >= rows.size()) {
            return;
        }
        List<Long> ids = rows.subList(fromIndex, rows.size()).stream()
                .map(ClaimedMessage::id)
                .toList();
        int released = messageRepository.releaseMessages(ids, workerId);
        if (released == ids.size()) {
            return;
        }
        for (Long id : ids) {
            warnOwnershipLost(id, "releaseMessages");
        }
    }

    private void warnOwnershipLost(long messageId, String operation) {
        if (!log.isWarnEnabled()) {
            return;
        }
        log.warn(
                "worker.ownership.lost mode={} workerId={} messageId={} operation={}",
                MODE_BLOCKING,
                workerId,
                messageId,
                operation);
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private enum MessageOutcome {
        NONE,
        PROCESSED,
        FAILED,
        STOPPED
    }

    private record ProcessResult(boolean continueKey, MessageOutcome outcome) {}

    static final class BatchOutcome {
        private final int batchSize;
        private int processed;
        private int failed;
        private int stopped;
        private long durationMs;

        BatchOutcome(int batchSize) {
            this.batchSize = batchSize;
        }

        void record(MessageOutcome outcome) {
            switch (outcome) {
                case PROCESSED -> processed++;
                case FAILED -> failed++;
                case STOPPED -> stopped++;
                case NONE -> {}
            }
        }

        void complete(long durationMs) {
            this.durationMs = durationMs;
        }

        int processed() {
            return processed;
        }

        int failed() {
            return failed;
        }

        int stopped() {
            return stopped;
        }

        int batchSize() {
            return batchSize;
        }

        long durationMs() {
            return durationMs;
        }
    }
}
