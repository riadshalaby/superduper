package net.rsworld.superduper.repository.api;

import java.util.List;

public interface WorkerMessageRepository {
    List<Long> claimBatch(String workerId, int batchSize, int maxRetries);

    List<ClaimedMessage> fetchClaimedByIds(List<Long> ids);

    void markProcessed(long id);

    void markReadyForRetry(long id, int retryCount);

    void markStopped(long id, int retryCount);
}
