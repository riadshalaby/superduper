package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Mono;

/** Reactive maintenance operations used by worker infrastructure. */
public interface ReactiveWorkerMaintenanceRepository {
    /**
     * Records or refreshes the heartbeat for the given worker.
     *
     * @param workerId the worker identifier
     * @return completion for the heartbeat write
     */
    Mono<Void> heartbeat(String workerId);

    default Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec) {
        return reclaimStaleProcessing(orphanTimeoutSec, "default");
    }

    /**
     * Reclaims processing rows whose claim has aged past the orphan timeout.
     *
     * @param orphanTimeoutSec the maximum age in seconds for a live claim
     * @return the number of rows moved back to READY
     */
    Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec, String topic);

    default Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return reclaimMissingHeartbeats(heartbeatWindowSec, "default");
    }

    /**
     * Reclaims processing rows whose worker no longer has a recent heartbeat.
     *
     * @param heartbeatWindowSec the heartbeat freshness window in seconds
     * @return the number of rows moved back to READY
     */
    Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec, String topic);

    default Mono<Integer> deleteProcessedOlderThan(int retentionDays) {
        return deleteProcessedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes processed messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
     * @return the number of deleted rows
     */
    Mono<Integer> deleteProcessedOlderThan(int retentionDays, String topic);

    default Mono<Integer> deleteStoppedOlderThan(int retentionDays) {
        return deleteStoppedOlderThan(retentionDays, "default");
    }

    /**
     * Deletes stopped messages older than the configured retention window.
     *
     * @param retentionDays the age threshold in days
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
