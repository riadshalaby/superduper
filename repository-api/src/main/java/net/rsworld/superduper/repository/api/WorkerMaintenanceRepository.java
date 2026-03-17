package net.rsworld.superduper.repository.api;

/** Provides blocking maintenance operations for worker heartbeats, reclamation, and cleanup. */
public interface WorkerMaintenanceRepository {
    /**
     * Records or refreshes the heartbeat for the given worker.
     *
     * @param workerId the worker identifier
     */
    void heartbeat(String workerId);

    /**
     * Reclaims stale processing rows for the default topic.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @return the number of rows moved back to READY
     */
    default int reclaimStaleProcessing(int orphanTimeoutSec) {
        return reclaimStaleProcessing(orphanTimeoutSec, "default");
    }

    /**
     * Reclaims processing rows whose claim has aged past the orphan timeout.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @param topic the topic whose processing rows should be reclaimed
     * @return the number of rows moved back to READY
     */
    int reclaimStaleProcessing(int orphanTimeoutSec, String topic);

    /**
     * Reclaims rows with missing heartbeats for the default topic.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @return the number of rows moved back to READY
     */
    default int reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return reclaimMissingHeartbeats(heartbeatWindowSec, "default");
    }

    /**
     * Reclaims processing rows whose worker no longer has a recent heartbeat.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @param topic the topic whose processing rows should be reclaimed
     * @return the number of rows moved back to READY
     */
    int reclaimMissingHeartbeats(int heartbeatWindowSec, String topic);

    /**
     * Deletes old processed messages for the default topic.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    default int deleteProcessedOlderThan(int retentionDays) {
        return deleteProcessedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes processed messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @param topic the topic whose processed rows should be deleted
     * @return the number of deleted rows
     */
    int deleteProcessedOlderThan(int retentionDays, String topic);

    /**
     * Deletes old stopped messages for the default topic.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    default int deleteStoppedOlderThan(int retentionDays) {
        return deleteStoppedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes stopped messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @param topic the topic whose stopped rows should be deleted
     * @return the number of deleted rows
     */
    int deleteStoppedOlderThan(int retentionDays, String topic);

    /**
     * Deletes stale heartbeat rows older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    int deleteStaleHeartbeats(int retentionDays);
}
