package net.rsworld.superduper.worker.reactive;

import jakarta.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.UUID;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class SuperDuperWorkerReactiveService {
    private static final Logger log = LoggerFactory.getLogger(SuperDuperWorkerReactiveService.class);

    private final ReactiveWorkerMessageRepository messageRepository;
    private final ReactiveMessageHandler handler;
    private final SuperduperObserver observer;
    private final String workerId;
    private final int batchSize;
    private final int maxRetries;
    private final long claimIntervalMs;

    public SuperDuperWorkerReactiveService(
            ReactiveWorkerMessageRepository messageRepository,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            @Value("${superduper.worker.batch-size:100}") int batchSize,
            @Value("${superduper.worker.max-retries:5}") int maxRetries,
            @Value("${superduper.worker.claim-interval-ms:5000}") long claimIntervalMs) {
        this.messageRepository = messageRepository;
        this.handler = handler;
        this.observer = observer;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.claimIntervalMs = claimIntervalMs;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName() + ":" + UUID.randomUUID();
    }

    @PostConstruct
    public void startLoops() {
        Flux.interval(Duration.ofMillis(claimIntervalMs))
                .concatMap(tick -> {
                    long started = System.nanoTime();
                    return messageRepository
                            .claimBatch(workerId, batchSize, maxRetries)
                            .collectList()
                            .flatMap(ids -> {
                                observer.workerClaimed(
                                        new WorkerObservation(
                                                "reactive", workerId, null, null, batchSize, elapsedMs(started)),
                                        ids.size());
                                return messageRepository.fetchClaimedByIds(ids).concatMap(this::processOne).then();
                            })
                            .onErrorResume(e -> {
                                observer.workerFailed(
                                        new WorkerObservation(
                                                "reactive", workerId, null, null, batchSize, elapsedMs(started)),
                                        e);
                                log.error("Reactive claim/process loop failed for tick {}", tick, e);
                                return Mono.empty();
                            });
                })
                .subscribe();
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
                    log.error("Reactive handler failed for message id={}", id, e);
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
