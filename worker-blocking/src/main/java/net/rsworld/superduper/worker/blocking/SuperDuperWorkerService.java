package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@SuppressWarnings("java:S107")
public class SuperDuperWorkerService {
    private static final String MODE_BLOCKING = "blocking";

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
        final long[] claimedCount = new long[] {0L};
        long claimStarted = System.nanoTime();
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> claimedCount[0] = claimBatch(),
                    new LockConfiguration(Instant.now(), claimLockName, lockAtMostFor, lockAtLeastFor));
            observer.workerClaimed(
                    new WorkerObservation(MODE_BLOCKING, workerId, null, null, batch, elapsedMs(claimStarted)),
                    Math.toIntExact(claimedCount[0]));
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation(MODE_BLOCKING, workerId, null, null, batch, elapsedMs(claimStarted)), e);
            return;
        }
        if (claimedCount[0] > 0) {
            process();
        }
    }

    long claimBatch() {
        Long claimed = tx.execute(st -> messageRepository.claimBatch(workerId, batch, maxRetries));
        return claimed == null ? 0L : claimed;
    }

    void process() {
        var rows = messageRepository.fetchClaimedForWorker(workerId);

        for (var row : rows) {
            long started = System.nanoTime();
            ProcessingResult res = ProcessingResult.RETRY;
            try {
                res = handler.handle(new MessageRow(
                        row.id(), null, row.key(), row.content(), "PROCESSING", row.retryCount(), row.containerId()));
            } catch (RuntimeException | MessageHandlingException e) {
                observer.workerFailed(
                        new WorkerObservation(
                                MODE_BLOCKING, workerId, row.id(), row.retryCount(), batch, elapsedMs(started)),
                        e);
            }
            int retry = row.retryCount() == null ? 0 : row.retryCount();
            if (res == ProcessingResult.SUCCESS) {
                messageRepository.markProcessed(row.id());
                observer.workerProcessed(
                        new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
            } else {
                retry++;
                if (retry < maxRetries) {
                    messageRepository.markReadyForRetry(row.id(), retry);
                    observer.workerRetried(
                            new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
                } else {
                    messageRepository.markStopped(row.id(), retry);
                    observer.workerStopped(
                            new WorkerObservation(MODE_BLOCKING, workerId, row.id(), retry, batch, elapsedMs(started)));
                }
            }
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
