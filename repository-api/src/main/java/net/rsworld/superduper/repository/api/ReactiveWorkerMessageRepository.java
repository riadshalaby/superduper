package net.rsworld.superduper.repository.api;

import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveWorkerMessageRepository {
    Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries);

    Flux<ClaimedMessage> fetchClaimedForWorker(String workerId);

    /**
     * Finds messages in the given terminal failure status ordered by id.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    Flux<ClaimedMessage> findByStatus(String status, int limit);

    Mono<Integer> releaseMessages(java.util.List<Long> ids, String containerId);

    Mono<Boolean> markProcessed(long id, String containerId);

    Mono<Boolean> markFailed(long id, int retryCount, String containerId);

    Mono<Boolean> markStopped(long id, int retryCount, String containerId);

    /**
     * Redrives a single failed or stopped message back to READY.
     *
     * @param id the message id
     * @return the number of updated rows
     */
    Mono<Integer> redriveById(long id);

    /**
     * Redrives up to the requested number of messages in the given terminal failure status.
     *
     * @param status the message status to redrive
     * @param limit the maximum number of rows to update
     * @return the number of updated rows
     */
    Mono<Integer> redriveByStatus(String status, int limit);

    /**
     * Counts queued messages grouped by status.
     *
     * @return a map of status to row count
     */
    Mono<Map<String, Long>> countByStatus();
}
