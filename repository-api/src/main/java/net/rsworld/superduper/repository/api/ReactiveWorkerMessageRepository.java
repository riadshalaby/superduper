package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveWorkerMessageRepository {
    Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries);

    Flux<ClaimedMessage> fetchClaimedForWorker(String workerId);

    Mono<Void> markProcessed(long id);

    Mono<Void> markReadyForRetry(long id, int retryCount);

    Mono<Void> markStopped(long id, int retryCount);
}
