package net.rsworld.superduper.repository.api;

import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveWorkerMessageRepository {
    Flux<Long> claimBatch(String workerId, int batchSize, int maxRetries);

    Flux<ClaimedMessage> fetchClaimedByIds(List<Long> ids);

    Mono<Void> markProcessed(long id);

    Mono<Void> markReadyForRetry(long id, int retryCount);

    Mono<Void> markStopped(long id, int retryCount);
}
