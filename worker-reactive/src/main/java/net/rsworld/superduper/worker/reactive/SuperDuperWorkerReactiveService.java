package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.springframework.beans.factory.annotation.Value;
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

    public SuperDuperWorkerReactiveService(
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            @Value("${superduper.worker.batch-size:100}") int batchSize,
            @Value("${superduper.worker.max-retries:5}") int maxRetries) {
        this.messageRepository = messageRepository;
        this.lockExec = lockExec;
        this.handler = handler;
        this.observer = observer;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName() + ":" + UUID.randomUUID();
    }

    @Scheduled(fixedDelayString = "${superduper.worker.claim-interval-ms:5000}", initialDelay = 3000)
    public void schedule() {
        long started = System.nanoTime();
        List<Long> ids = new ArrayList<>();
        try {
            lockExec.executeWithLock(
                    (Runnable) () -> ids.addAll(claimBatch()),
                    new LockConfiguration(
                            Instant.now(), "superduper-claim-batch", Duration.ofSeconds(15), Duration.ofSeconds(2)));
            observer.workerClaimed(
                    new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)), ids.size());
        } catch (RuntimeException e) {
            observer.workerFailed(
                    new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)), e);
            return;
        }

        if (ids.isEmpty()) {
            return;
        }

        messageRepository
                .fetchClaimedByIds(ids)
                .concatMap(this::processOne)
                .onErrorResume(e -> {
                    observer.workerFailed(
                            new WorkerObservation("reactive", workerId, null, null, batchSize, elapsedMs(started)), e);
                    return Mono.empty();
                })
                .then()
                .block();
    }

    List<Long> claimBatch() {
        return messageRepository
                .claimBatch(workerId, batchSize, maxRetries)
                .collectList()
                .blockOptional()
                .orElseGet(List::of);
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
