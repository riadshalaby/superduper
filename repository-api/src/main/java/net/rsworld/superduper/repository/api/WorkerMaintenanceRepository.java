package net.rsworld.superduper.repository.api;

/** Blocking maintenance operations used by worker infrastructure. */
public interface WorkerMaintenanceRepository {
    /**
     * Records or refreshes the heartbeat for the given worker.
     *
     * @param workerId the worker identifier
     */
    void heartbeat(String workerId);

    /**
     * Reclaims processing rows whose claim has aged past the orphan timeout.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @return the number of rows moved back to READY
     */
    int reclaimStaleProcessing(int orphanTimeoutSec);

    /**
     * Reclaims processing rows whose worker no longer has a recent heartbeat.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @return the number of rows moved back to READY
     */
    int reclaimMissingHeartbeats(int heartbeatWindowSec);

    /**
     * Deletes processed messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    int deleteProcessedOlderThan(int retentionDays);

    /**
     * Deletes stopped messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    int deleteStoppedOlderThan(int retentionDays);

    /**
     * Deletes stale heartbeat rows older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    int deleteStaleHeartbeats(int retentionDays);
}
