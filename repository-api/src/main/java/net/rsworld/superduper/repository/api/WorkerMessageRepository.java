package net.rsworld.superduper.repository.api;

import java.util.List;
import java.util.Map;

public interface WorkerMessageRepository {
    long claimBatch(String workerId, int batchSize, int maxRetries);

    List<ClaimedMessage> fetchClaimedForWorker(String workerId);

    /**
     * Finds messages in the given terminal failure status ordered by id.
     *
     * @param status the message status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    List<ClaimedMessage> findByStatus(String status, int limit);

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
    int redriveByStatus(String status, int limit);

    /**
     * Counts queued messages grouped by status.
     *
     * @return a map of status to row count
     */
    Map<String, Long> countByStatus();
}
