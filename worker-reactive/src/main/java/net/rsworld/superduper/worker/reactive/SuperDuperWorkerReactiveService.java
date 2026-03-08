package net.rsworld.superduper.worker.reactive;

import jakarta.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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
import reactor.core.publisher.Flux;
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
                .collectList()
                .flatMap(this::processBatch)
                .doOnSuccess(outcome -> observer.workerBatchCompleted(
                        new WorkerObservation(
                                MODE_REACTIVE, workerId, null, null, outcome.batchSize(), outcome.durationMs()),
                        outcome.processed(),
                        outcome.failed(),
                        outcome.stopped()))
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

    private Mono<BatchOutcome> processBatch(List<ClaimedMessage> rows) {
        long started = System.nanoTime();
        BatchOutcome outcome = new BatchOutcome(rows.size());
        return Flux.fromIterable(groupRowsByMessageKey(rows).values())
                .concatMap(keyGroup -> processKeyGroup(keyGroup, outcome))
                .then(Mono.fromSupplier(() -> {
                    outcome.complete(elapsedMs(started));
                    return outcome;
                }));
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

    private Mono<Void> processKeyGroup(List<ClaimedMessage> rows, BatchOutcome outcome) {
        return processKeyGroup(rows, 0, outcome);
    }

    private Mono<Void> processKeyGroup(List<ClaimedMessage> rows, int index, BatchOutcome outcome) {
        if (index >= rows.size()) {
            return Mono.empty();
        }
        ClaimedMessage row = rows.get(index);
        return handleClaimedMessage(row).flatMap(result -> {
            outcome.record(result.outcome());
            if (result.continueKey()) {
                return processKeyGroup(rows, index + 1, outcome);
            }
            return releaseRemaining(rows, index + 1);
        });
    }

    private Mono<Void> processOne(ClaimedMessage row) {
        return handleClaimedMessage(row).then();
    }

    private Mono<ProcessResult> handleClaimedMessage(ClaimedMessage row) {
        long started = System.nanoTime();
        long id = row.id();
        int retry = row.retryCount() == null ? 0 : row.retryCount();
        MessageRow messageRow = new MessageRow(
                id,
                row.messageId(),
                row.messageKey(),
                row.content(),
                "PROCESSING",
                retry,
                row.containerId(),
                row.correlationId(),
                row.messageType());
        return handler.handle(messageRow)
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
                                return Mono.just(new ProcessResult(true, MessageOutcome.NONE));
                            }
                            observer.workerProcessed(new WorkerObservation(
                                    MODE_REACTIVE, workerId, id, retry, batchSize, elapsedMs(started)));
                            return Mono.just(new ProcessResult(true, MessageOutcome.PROCESSED));
                        });
                    }

                    if (outcome.failureCause() == null) {
                        observer.workerFailed(
                                new WorkerObservation(
                                        MODE_REACTIVE, workerId, id, retry, batchSize, elapsedMs(started)),
                                new IllegalStateException("Message handler returned FAILURE"));
                    }
                    int next = retry + 1;
                    if (next < maxRetries) {
                        return messageRepository.markFailed(id, next, workerId).flatMap(updated -> {
                            if (!updated) {
                                warnOwnershipLost(id, "markFailed");
                                return Mono.just(new ProcessResult(false, MessageOutcome.NONE));
                            }
                            observer.workerRetried(new WorkerObservation(
                                    MODE_REACTIVE, workerId, id, next, batchSize, elapsedMs(started)));
                            return Mono.just(new ProcessResult(false, MessageOutcome.FAILED));
                        });
                    }
                    return messageRepository.markStopped(id, next, workerId).flatMap(updated -> {
                        if (!updated) {
                            warnOwnershipLost(id, "markStopped");
                            return Mono.just(new ProcessResult(false, MessageOutcome.NONE));
                        }
                        observer.workerStopped(new WorkerObservation(
                                MODE_REACTIVE, workerId, id, next, batchSize, elapsedMs(started)));
                        return Mono.just(new ProcessResult(false, MessageOutcome.STOPPED));
                    });
                });
    }

    private Mono<Void> releaseRemaining(List<ClaimedMessage> rows, int fromIndex) {
        if (fromIndex >= rows.size()) {
            return Mono.empty();
        }
        List<Long> ids = rows.subList(fromIndex, rows.size()).stream()
                .map(ClaimedMessage::id)
                .toList();
        return messageRepository
                .releaseMessages(ids, workerId)
                .doOnNext(released -> {
                    if (released == ids.size()) {
                        return;
                    }
                    for (Long id : ids) {
                        warnOwnershipLost(id, "releaseMessages");
                    }
                })
                .then();
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

    private enum MessageOutcome {
        NONE,
        PROCESSED,
        FAILED,
        STOPPED
    }

    private record ProcessResult(boolean continueKey, MessageOutcome outcome) {}

    static final class BatchOutcome {
        private final int batchSize;
        private final AtomicInteger processed = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicInteger stopped = new AtomicInteger();
        private final AtomicLong durationMs = new AtomicLong();

        BatchOutcome(int batchSize) {
            this.batchSize = batchSize;
        }

        void record(MessageOutcome outcome) {
            switch (outcome) {
                case PROCESSED -> processed.incrementAndGet();
                case FAILED -> failed.incrementAndGet();
                case STOPPED -> stopped.incrementAndGet();
                case NONE -> {}
            }
        }

        void complete(long durationMs) {
            this.durationMs.set(durationMs);
        }

        int processed() {
            return processed.get();
        }

        int failed() {
            return failed.get();
        }

        int stopped() {
            return stopped.get();
        }

        int batchSize() {
            return batchSize;
        }

        long durationMs() {
            return durationMs.get();
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
