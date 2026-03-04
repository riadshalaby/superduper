package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveWorkerMessageRepository {
    Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries);

    Flux<ClaimedMessage> fetchClaimedForWorker(String workerId);

    Mono<Boolean> markProcessed(long id, String containerId);

    Mono<Boolean> markFailed(long id, int retryCount, String containerId);

    Mono<Boolean> markStopped(long id, int retryCount, String containerId);
}
