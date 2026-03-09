package net.rsworld.superduper.observability.api;

import java.util.Map;

public interface SuperduperObserver {

    void consumerReceived(ConsumerObservation observation);

    void consumerSucceeded(ConsumerObservation observation);

    void consumerFailed(ConsumerObservation observation, Throwable error);

    void workerClaimed(WorkerObservation observation, int claimedCount);

    void workerProcessed(WorkerObservation observation);

    /**
     * Records a batch-level worker summary after processing completes.
     *
     * @param observation the batch observation context
     * @param processed the number of successfully processed messages
     * @param failed the number of retried messages
     * @param stopped the number of stopped messages
     */
    default void workerBatchCompleted(WorkerObservation observation, int processed, int failed, int stopped) {}

    void workerRetried(WorkerObservation observation);

    void workerStopped(WorkerObservation observation);

    /**
     * Records an administrative redrive action.
     *
     * @param observation the redrive observation context
     * @param redrivenCount the number of messages moved back to READY
     */
    default void workerRedriven(WorkerObservation observation, int redrivenCount) {}

    void workerFailed(WorkerObservation observation, Throwable error);

    void maintenanceSucceeded(MaintenanceObservation observation);

    /**
     * Records a successful maintenance action with reclaimed row count.
     *
     * @param observation the maintenance observation context
     * @param reclaimedCount the number of reclaimed rows
     */
    default void maintenanceSucceeded(MaintenanceObservation observation, int reclaimedCount) {
        maintenanceSucceeded(observation);
    }

    /**
     * Records queue backlog counts grouped by status.
     *
     * @param mode the worker mode
     * @param statusCounts queue counts grouped by status
     */
    default void queueBacklogObserved(String mode, Map<String, Long> statusCounts) {}

    /**
     * Records a cleanup maintenance result.
     *
     * @param observation the cleanup observation context
     * @param deletedCount the number of deleted rows
     */
    default void maintenanceCleanup(MaintenanceObservation observation, int deletedCount) {}

    void maintenanceFailed(MaintenanceObservation observation, Throwable error);
}
