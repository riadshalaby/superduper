package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class SuperDuperWorkerReactiveService {
    private final ReactiveWorkerMessageRepository messageRepository;
    private final LockingTaskExecutor lockExec;
    private final ReactiveMessageHandler handler;
    private final SuperduperObserver observer;
    private final String workerId;
    private final int batchSize;
    private final int maxRetries;
    private final String claimLockName;
    private final Duration lockAtMostFor;
    private final Duration lockAtLeastFor;

    public SuperDuperWorkerReactiveService(
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            int batchSize,
            int maxRetries,
            String claimLockName,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        this.messageRepository = messageRepository;
        this.lockExec = lockExec;
        this.handler = handler;
        this.observer = observer;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.claimLockName = claimLockName;
        this.lockAtMostFor = Duration.ofMillis(lockAtMostForMs);
        this.lockAtLeastFor = Duration.ofMillis(lockAtLeastForMs);
        this.workerId = ManagementFactory.getRuntimeMXBean().getName() + ":" + UUID.randomUUID();
    }

    public SuperDuperWorkerReactiveService(
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            int batchSize,
            int maxRetries) {
        this(
                messageRepository,
                lockExec,
                handler,
                observer,
                batchSize,
                maxRetries,
                "superduper-claim-batch",
                15000,
                2000);
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.claim-interval-ms:5000}",
            initialDelayString = "${superduper.worker.claim-initial-delay-ms:3000}")
    public void schedule() {
        long started = System.nanoTime();
        long[] claimedCount = new long[] {0L};
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> claimedCount[0] = claimBatch(),
                    new LockConfiguration(Instant.now(), claimLockName, lockAtMostFor, lockAtLeastFor));
            observer.workerClaimed(
                    new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)),
                    Math.toIntExact(claimedCount[0]));
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)), e);
            return;
        }

        if (claimedCount[0] <= 0) {
            return;
        }

        messageRepository
                .fetchClaimedForWorker(workerId)
                .concatMap(this::processOne)
                .onErrorResume(e -> {
                    observer.workerFailed(
                            new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)), e);
                    return Mono.empty();
                })
                .then()
                .block();
    }

    long claimBatch() {
        return messageRepository
                .claimBatch(workerId, batchSize, maxRetries)
                .blockOptional()
                .orElse(0L);
    }

    private Mono<Void> processOne(ClaimedMessage row) {
        long started = System.nanoTime();
        long id = row.id();
        int retry = row.retryCount() == null ? 0 : row.retryCount();
        MessageRow mr = new MessageRow(id, null, row.key(), row.content(), "PROCESSING", retry, workerId);
        return handler.handle(mr)
                .onErrorResume(e -> {
                    observer.workerFailed(
                            new WorkerObservation("reactive", workerId, id, retry, batchSize, elapsedMs(started)), e);
                    return Mono.just(ProcessingResult.RETRY);
                })
                .flatMap(result -> {
                    if (result == ProcessingResult.SUCCESS) {
                        return messageRepository
                                .markProcessed(id)
                                .then(Mono.fromRunnable(() -> observer.workerProcessed(new WorkerObservation(
                                        "reactive", workerId, id, retry, batchSize, elapsedMs(started)))));
                    } else {
                        int next = retry + 1;
                        if (next < maxRetries) {
                            return messageRepository
                                    .markReadyForRetry(id, next)
                                    .then(Mono.fromRunnable(() -> observer.workerRetried(new WorkerObservation(
                                            "reactive", workerId, id, next, batchSize, elapsedMs(started)))));
                        } else {
                            return messageRepository
                                    .markStopped(id, next)
                                    .then(Mono.fromRunnable(() -> observer.workerStopped(new WorkerObservation(
                                            "reactive", workerId, id, next, batchSize, elapsedMs(started)))));
                        }
                    }
                });
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
