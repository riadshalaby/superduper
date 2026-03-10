package net.rsworld.superduper.repository.api;

import java.util.List;
import java.util.Map;

public interface WorkerMessageRepository {
    default long claimBatch(String workerId, int batchSize, int maxRetries) {
        return claimBatch(workerId, batchSize, maxRetries, "default");
    }

    long claimBatch(String workerId, int batchSize, int maxRetries, String topic);

    default List<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return fetchClaimedForWorker(workerId, "default");
    }

    List<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic);

    /**
     * Finds messages in the given terminal failure status ordered by id.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    default List<ClaimedMessage> findByStatus(String status, int limit) {
        return findByStatus(status, limit, "default");
    }

    List<ClaimedMessage> findByStatus(String status, int limit, String topic);

    int releaseMessages(List<Long> ids, String containerId);

    boolean markProcessed(long id, String containerId);

    boolean markFailed(long id, int retryCount, String containerId);

    boolean markStopped(long id, int retryCount, String containerId);

    /**
     * Redrives a single failed or stopped message back to READY.
     *
     * @param id the message id
     * @return the number of updated rows
     */
    int redriveById(long id);

    /**
     * Redrives up to the requested number of messages in the given terminal failure status.
     *
     * @param status the message status to redrive
     * @param limit the maximum number of rows to update
     * @return the number of updated rows
     */
    default int redriveByStatus(String status, int limit) {
        return redriveByStatus(status, limit, "default");
    }

    int redriveByStatus(String status, int limit, String topic);

    /**
     * Counts queued messages grouped by status.
     *
     * @return a map of status to row count
     */
    default Map<String, Long> countByStatus() {
        return countByStatus("default");
    }

    Map<String, Long> countByStatus(String topic);
}
