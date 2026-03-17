package net.rsworld.superduper.repository.api;

import java.util.List;
import java.util.Map;

/** Provides blocking worker operations for claiming, inspecting, and updating queued messages. */
public interface WorkerMessageRepository {
    /**
     * Claims a batch of messages for the default topic.
     *
     * @param workerId the worker identifier claiming the batch
     * @param batchSize the maximum number of rows to claim
     * @param maxRetries the maximum retry count allowed for claimable rows
     * @return the number of claimed rows
     */
    default long claimBatch(String workerId, int batchSize, int maxRetries) {
        return claimBatch(workerId, batchSize, maxRetries, "default");
    }

    /**
     * Claims a batch of messages for a specific topic.
     *
     * @param workerId the worker identifier claiming the batch
     * @param batchSize the maximum number of rows to claim
     * @param maxRetries the maximum retry count allowed for claimable rows
     * @param topic the topic whose rows should be claimed
     * @return the number of claimed rows
     */
    long claimBatch(String workerId, int batchSize, int maxRetries, String topic);

    /**
     * Fetches the messages currently claimed by a worker for the default topic.
     *
     * @param workerId the worker identifier whose claimed rows should be fetched
     * @return the claimed messages for that worker
     */
    default List<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return fetchClaimedForWorker(workerId, "default");
    }

    /**
     * Fetches the messages currently claimed by a worker for a specific topic.
     *
     * @param workerId the worker identifier whose claimed rows should be fetched
     * @param topic the topic whose claimed rows should be returned
     * @return the claimed messages for that worker
     */
    List<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic);

    /**
     * Finds messages in the given terminal failure status ordered by id for the default topic.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    default List<ClaimedMessage> findByStatus(String status, int limit) {
        return findByStatus(status, limit, "default");
    }

    /**
     * Finds messages in the given terminal failure status ordered by id for a specific topic.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @param topic the topic whose rows should be searched
     * @return matching rows ordered by id
     */
    List<ClaimedMessage> findByStatus(String status, int limit, String topic);

    /**
     * Releases the claimed rows held by a worker container.
     *
     * @param ids the claimed message identifiers to release
     * @param containerId the worker container that currently owns the rows
     * @return the number of released rows
     */
    int releaseMessages(List<Long> ids, String containerId);

    /**
     * Marks a claimed message as processed successfully.
     *
     * @param id the message identifier
     * @param containerId the worker container that owns the row
     * @return {@code true} when the row was updated
     */
    boolean markProcessed(long id, String containerId);

    /**
     * Marks a claimed message as failed and eligible for retry.
     *
     * @param id the message identifier
     * @param retryCount the retry count to persist
     * @param containerId the worker container that owns the row
     * @return {@code true} when the row was updated
     */
    boolean markFailed(long id, int retryCount, String containerId);

    /**
     * Marks a claimed message as stopped after exhausting retries.
     *
     * @param id the message identifier
     * @param retryCount the retry count to persist
     * @param containerId the worker container that owns the row
     * @return {@code true} when the row was updated
     */
    boolean markStopped(long id, int retryCount, String containerId);

    /**
     * Redrives a single failed or stopped message back to READY.
     *
     * @param id the message id
     * @return the number of updated rows
     */
    int redriveById(long id);

    /**
     * Redrives up to the requested number of messages in the given terminal failure status for the default topic.
     *
     * @param status the message status to redrive
     * @param limit the maximum number of rows to update
     * @return the number of updated rows
     */
    default int redriveByStatus(String status, int limit) {
        return redriveByStatus(status, limit, "default");
    }

    /**
     * Redrives up to the requested number of messages in the given terminal failure status.
     *
     * @param status the message status to redrive
     * @param limit the maximum number of rows to update
     * @param topic the topic whose rows should be redriven
     * @return the number of updated rows
     */
    int redriveByStatus(String status, int limit, String topic);

    /**
     * Counts queued messages grouped by status for the default topic.
     *
     * @return a map of status to row count
     */
    default Map<String, Long> countByStatus() {
        return countByStatus("default");
    }

    /**
     * Counts queued messages grouped by status for a specific topic.
     *
     * @param topic the topic whose counts should be returned
     * @return a map of status to row count
     */
    Map<String, Long> countByStatus(String topic);
}
