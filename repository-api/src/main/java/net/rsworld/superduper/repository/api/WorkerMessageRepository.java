package net.rsworld.superduper.repository.api;

import java.util.List;

public interface WorkerMessageRepository {
    long claimBatch(String workerId, int batchSize, int maxRetries);

    List<ClaimedMessage> fetchClaimedForWorker(String workerId);

    int releaseMessages(List<Long> ids, String containerId);

    boolean markProcessed(long id, String containerId);

    boolean markFailed(long id, int retryCount, String containerId);

    boolean markStopped(long id, int retryCount, String containerId);
}
