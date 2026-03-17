package net.rsworld.superduper.repository.api;

import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive worker repository contract for claiming, updating, and inspecting queued messages.
 *
 * <p>Reactive workers use this port to drive the full message lifecycle from {@code READY} through terminal states.
 */
public interface ReactiveWorkerMessageRepository {
    /**
     * Claims a batch of messages from the shared default topic.
     *
     * @param workerId the worker identifier taking ownership of the batch
     * @param batchSize the maximum number of rows to claim
     * @param maxRetries the highest retry count still eligible for claiming
     * @return the number of claimed rows
     */
    default Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries) {
        return claimBatch(workerId, batchSize, maxRetries, "default");
    }

    /**
     * Claims a batch of messages from the given logical topic.
     *
     * @param workerId the worker identifier taking ownership of the batch
     * @param batchSize the maximum number of rows to claim
     * @param maxRetries the highest retry count still eligible for claiming
     * @param topic the logical topic to claim from
     * @return the number of claimed rows
     */
    Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries, String topic);

    /**
     * Fetches the rows currently claimed by the worker from the shared default topic.
     *
     * @param workerId the worker identifier
     * @return the claimed rows in processing order
     */
    default Flux<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return fetchClaimedForWorker(workerId, "default");
    }

    /**
     * Fetches the rows currently claimed by the worker from the given logical topic.
     *
     * @param workerId the worker identifier
     * @param topic the logical topic to inspect
     * @return the claimed rows in processing order
     */
    Flux<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic);

    /**
     * Finds messages in the given terminal failure status ordered by id.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    default Flux<ClaimedMessage> findByStatus(String status, int limit) {
        return findByStatus(status, limit, "default");
    }

    /**
     * Finds messages in the given terminal failure status ordered by id for the supplied topic.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @param topic the logical topic to inspect
     * @return matching rows ordered by id
     */
    Flux<ClaimedMessage> findByStatus(String status, int limit, String topic);

    /**
     * Releases claimed messages back to {@code READY}.
     *
     * @param ids the message ids to release
     * @param containerId the container id that currently owns the claims
     * @return the number of released rows
     */
    Mono<Integer> releaseMessages(java.util.List<Long> ids, String containerId);

    /**
     * Marks a claimed message as processed successfully.
     *
     * @param id the message row id
     * @param containerId the container id that currently owns the claim
     * @return {@code true} when the row was updated
     */
    Mono<Boolean> markProcessed(long id, String containerId);

    /**
     * Marks a claimed message as failed and increments its retry count.
     *
     * @param id the message row id
     * @param retryCount the retry count to persist
     * @param containerId the container id that currently owns the claim
     * @return {@code true} when the row was updated
     */
    Mono<Boolean> markFailed(long id, int retryCount, String containerId);

    /**
     * Marks a claimed message as permanently stopped.
     *
     * @param id the message row id
     * @param retryCount the retry count to persist
     * @param containerId the container id that currently owns the claim
     * @return {@code true} when the row was updated
     */
    Mono<Boolean> markStopped(long id, int retryCount, String containerId);

    /**
     * Redrives a single failed or stopped message back to {@code READY}.
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
    default Mono<Integer> redriveByStatus(String status, int limit) {
        return redriveByStatus(status, limit, "default");
    }

    /**
     * Redrives up to the requested number of terminally failed messages for the supplied topic.
     *
     * @param status the message status to redrive
     * @param limit the maximum number of rows to update
     * @param topic the logical topic to redrive in
     * @return the number of updated rows
     */
    Mono<Integer> redriveByStatus(String status, int limit, String topic);

    /**
     * Counts queued messages grouped by status.
     *
     * @return a map of status to row count
     */
    default Mono<Map<String, Long>> countByStatus() {
        return countByStatus("default");
    }

    /**
     * Counts queued messages grouped by status for the supplied topic.
     *
     * @param topic the logical topic to inspect
     * @return a map of status to row count
     */
    Mono<Map<String, Long>> countByStatus(String topic);
}
