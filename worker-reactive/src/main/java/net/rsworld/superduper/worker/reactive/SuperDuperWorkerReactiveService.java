package net.rsworld.superduper.worker.reactive;

import jakarta.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Service
@SuppressWarnings("java:S107")
public class SuperDuperWorkerReactiveService {
    private static final String MODE_REACTIVE = "reactive";
    private static final Logger log = LoggerFactory.getLogger(SuperDuperWorkerReactiveService.class);

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
    private final ExecutorService vtExecutor;
    private final Scheduler vtScheduler;

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
        this(
                messageRepository,
                lockExec,
                handler,
                observer,
                batchSize,
                maxRetries,
                claimLockName,
                lockAtMostForMs,
                lockAtLeastForMs,
                ManagementFactory.getRuntimeMXBean().getName());
    }

    SuperDuperWorkerReactiveService(
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            int batchSize,
            int maxRetries,
            String claimLockName,
            long lockAtMostForMs,
            long lockAtLeastForMs,
            String workerId) {
        this.messageRepository = messageRepository;
        this.lockExec = lockExec;
        this.handler = handler;
        this.observer = observer;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.claimLockName = claimLockName;
        this.lockAtMostFor = Duration.ofMillis(lockAtMostForMs);
        this.lockAtLeastFor = Duration.ofMillis(lockAtLeastForMs);
        this.workerId = workerId;
        this.vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.vtScheduler = Schedulers.fromExecutorService(vtExecutor);
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
        AtomicLong claimedCount = new AtomicLong(0L);
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> claimedCount.set(claimBatch()),
                    new LockConfiguration(Instant.now(), claimLockName, lockAtMostFor, lockAtLeastFor));
            observer.workerClaimed(
                    new WorkerObservation(MODE_REACTIVE, workerId, null, null, batchSize, elapsedMs(started)),
                    Math.toIntExact(claimedCount.get()));
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation(MODE_REACTIVE, workerId, null, null, batchSize, elapsedMs(started)), e);
            return;
        }

        if (claimedCount.get() <= 0) {
            return;
        }

        messageRepository
                .fetchClaimedForWorker(workerId)
                .concatMap(this::processOne)
                .onErrorResume(e -> {
                    observer.workerFailed(
                            new WorkerObservation(MODE_REACTIVE, workerId, null, null, batchSize, elapsedMs(started)),
                            e);
                    return Mono.empty();
                })
                .then()
                .subscribeOn(vtScheduler)
                .block();
    }

    long claimBatch() {
        return messageRepository
                .claimBatch(workerId, batchSize, maxRetries)
                .blockOptional()
                .orElse(0L);
    }

    @PreDestroy
    void shutdownVirtualThreadScheduler() {
        vtScheduler.dispose();
        vtExecutor.shutdown();
    }

    private Mono<Void> processOne(ClaimedMessage row) {
        long started = System.nanoTime();
        long id = row.id();
        int retry = row.retryCount() == null ? 0 : row.retryCount();
        MessageRow mr = new MessageRow(id, null, row.key(), row.content(), "PROCESSING", retry, workerId);
        return handler.handle(mr)
                .map(result -> new HandlerOutcome(result, null))
                .onErrorResume(e -> {
                    observer.workerFailed(
                            new WorkerObservation(MODE_REACTIVE, workerId, id, retry, batchSize, elapsedMs(started)),
                            e);
                    return Mono.just(new HandlerOutcome(ProcessingResult.FAILURE, e));
                })
                .flatMap(outcome -> {
                    if (outcome.result() == ProcessingResult.SUCCESS) {
                        return messageRepository.markProcessed(id, workerId).flatMap(updated -> {
                            if (!updated) {
                                warnOwnershipLost(id, "markProcessed");
                                return Mono.empty();
                            }
                            observer.workerProcessed(new WorkerObservation(
                                    MODE_REACTIVE, workerId, id, retry, batchSize, elapsedMs(started)));
                            return Mono.empty();
                        });
                    } else {
                        if (outcome.failureCause() == null) {
                            observer.workerFailed(
                                    new WorkerObservation(
                                            MODE_REACTIVE, workerId, id, retry, batchSize, elapsedMs(started)),
                                    new IllegalStateException("Message handler returned FAILURE"));
                        }
                        int next = retry + 1;
                        if (next < maxRetries) {
                            return messageRepository
                                    .markFailed(id, next, workerId)
                                    .flatMap(updated -> {
                                        if (!updated) {
                                            warnOwnershipLost(id, "markFailed");
                                            return Mono.empty();
                                        }
                                        observer.workerRetried(new WorkerObservation(
                                                MODE_REACTIVE, workerId, id, next, batchSize, elapsedMs(started)));
                                        return Mono.empty();
                                    });
                        } else {
                            return messageRepository
                                    .markStopped(id, next, workerId)
                                    .flatMap(updated -> {
                                        if (!updated) {
                                            warnOwnershipLost(id, "markStopped");
                                            return Mono.empty();
                                        }
                                        observer.workerStopped(new WorkerObservation(
                                                MODE_REACTIVE, workerId, id, next, batchSize, elapsedMs(started)));
                                        return Mono.empty();
                                    });
                        }
                    }
                });
    }

    private void warnOwnershipLost(long messageId, String operation) {
        if (!log.isWarnEnabled()) {
            return;
        }
        log.warn(
                "worker.ownership.lost mode={} workerId={} messageId={} operation={}",
                MODE_REACTIVE,
                workerId,
                messageId,
                operation);
    }

    private record HandlerOutcome(ProcessingResult result, Throwable failureCause) {}

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
