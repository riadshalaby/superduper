package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Mono;

/**
 * Reactive maintenance operations used by worker infrastructure.
 *
 * <p>Worker-side housekeeping services use this port to maintain heartbeats, reclaim orphaned claims, and clean up
 * expired rows.
 */
public interface ReactiveWorkerMaintenanceRepository {
    /**
     * Records or refreshes the heartbeat for the given worker.
     *
     * @param workerId the worker identifier
     * @return completion for the heartbeat write
     */
    Mono<Void> heartbeat(String workerId);

    /**
     * Reclaims stale processing rows from the shared default topic.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @return the number of rows moved back to {@code READY}
     */
    default Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec) {
        return reclaimStaleProcessing(orphanTimeoutSec, "default");
    }

    /**
     * Reclaims processing rows whose claim has aged past the orphan timeout.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @param topic the logical topic to reclaim from
     * @return the number of rows moved back to {@code READY}
     */
    Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec, String topic);

    /**
     * Reclaims rows whose worker heartbeat is missing from the shared default topic.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @return the number of rows moved back to {@code READY}
     */
    default Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return reclaimMissingHeartbeats(heartbeatWindowSec, "default");
    }

    /**
     * Reclaims processing rows whose worker no longer has a recent heartbeat.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @param topic the logical topic to reclaim from
     * @return the number of rows moved back to {@code READY}
     */
    Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec, String topic);

    /**
     * Deletes processed messages older than the configured retention window from the shared default topic.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    default Mono<Integer> deleteProcessedOlderThan(int retentionDays) {
        return deleteProcessedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes processed messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @param topic the logical topic to delete from
     * @return the number of deleted rows
     */
    Mono<Integer> deleteProcessedOlderThan(int retentionDays, String topic);

    /**
     * Deletes stopped messages older than the configured retention window from the shared default topic.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    default Mono<Integer> deleteStoppedOlderThan(int retentionDays) {
        return deleteStoppedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes stopped messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @param topic the logical topic to delete from
     * @return the number of deleted rows
     */
    Mono<Integer> deleteStoppedOlderThan(int retentionDays, String topic);

    /**
     * Deletes stale heartbeat rows older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    Mono<Integer> deleteStaleHeartbeats(int retentionDays);
}
