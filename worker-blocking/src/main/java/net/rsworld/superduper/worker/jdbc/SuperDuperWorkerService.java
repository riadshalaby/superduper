package net.rsworld.superduper.worker.jdbc;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class SuperDuperWorkerService {

    private final WorkerMessageRepository messageRepository;
    private final TransactionTemplate tx;
    private final LockingTaskExecutor lockExec;
    private final MessageHandler handler;
    private final SuperduperObserver observer;
    private final String workerId;
    private final int batch;
    private final int maxRetries;

    public SuperDuperWorkerService(
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            @Value("${superduper.worker.batch-size:100}") int batch,
            @Value("${superduper.worker.max-retries:5}") int maxRetries) {
        this.messageRepository = messageRepository;
        this.tx = new TransactionTemplate(txm);
        this.lockExec = lockExec;
        this.handler = handler;
        this.observer = observer;
        this.batch = batch;
        this.maxRetries = maxRetries;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(fixedDelayString = "${superduper.worker.claim-interval-ms:5000}", initialDelay = 3000)
    public void schedule() {
        final List<Long> ids = new ArrayList<>();
        long claimStarted = System.nanoTime();
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> ids.addAll(claimBatch()),
                    new LockConfiguration(
                            Instant.now(), "superduper-claim-batch", Duration.ofSeconds(15), Duration.ofSeconds(2)));
            observer.workerClaimed(
                    new WorkerObservation("blocking", workerId, null, null, batch, elapsedMs(claimStarted)),
                    ids.size());
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation("blocking", workerId, null, null, batch, elapsedMs(claimStarted)), e);
            return;
        }
        if (!ids.isEmpty()) process(ids);
    }

    List<Long> claimBatch() {
        return tx.execute(st -> messageRepository.claimBatch(workerId, batch, maxRetries));
    }

    void process(List<Long> ids) {
        var rows = messageRepository.fetchClaimedByIds(ids);

        for (var row : rows) {
            long started = System.nanoTime();
            ProcessingResult res = ProcessingResult.RETRY;
            try {
                res = handler.handle(new MessageRow(
                        row.id(), null, row.key(), row.content(), "PROCESSING", row.retryCount(), row.containerId()));
            } catch (Exception e) {
                observer.workerFailed(
                        new WorkerObservation(
                                "blocking", workerId, row.id(), row.retryCount(), batch, elapsedMs(started)),
                        e);
            }
            int retry = row.retryCount() == null ? 0 : row.retryCount();
            if (res == ProcessingResult.SUCCESS) {
                messageRepository.markProcessed(row.id());
                observer.workerProcessed(
                        new WorkerObservation("blocking", workerId, row.id(), retry, batch, elapsedMs(started)));
            } else {
                retry++;
                if (retry < maxRetries) {
                    messageRepository.markReadyForRetry(row.id(), retry);
                    observer.workerRetried(
                            new WorkerObservation("blocking", workerId, row.id(), retry, batch, elapsedMs(started)));
                } else {
                    messageRepository.markStopped(row.id(), retry);
                    observer.workerStopped(
                            new WorkerObservation("blocking", workerId, row.id(), retry, batch, elapsedMs(started)));
                }
            }
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
