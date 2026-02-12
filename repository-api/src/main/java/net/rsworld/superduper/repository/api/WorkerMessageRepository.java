package net.rsworld.superduper.repository.api;

import java.util.List;

public interface WorkerMessageRepository {
    long claimBatch(String workerId, int batchSize, int maxRetries);

    List<ClaimedMessage> fetchClaimedForWorker(String workerId);

    void markProcessed(long id);

    void markReadyForRetry(long id, int retryCount);

    void markStopped(long id, int retryCount);
}
