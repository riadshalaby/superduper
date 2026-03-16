package net.rsworld.superduper.observability.api;

import java.util.Map;

/** Receives lifecycle notifications from consumers, workers, and maintenance routines. */
public interface SuperduperObserver {

    /**
     * Records that a consumer accepted a message from the upstream broker.
     *
     * @param observation the consumer observation context
     */
    void consumerReceived(ConsumerObservation observation);

    /**
     * Records that a consumer stored the received message successfully.
     *
     * @param observation the consumer observation context
     */
    void consumerSucceeded(ConsumerObservation observation);

    /**
     * Records that a consumer failed while storing or handling a message.
     *
     * @param observation the consumer observation context
     * @param error the failure that interrupted processing
     */
    void consumerFailed(ConsumerObservation observation, Throwable error);

    /**
     * Records that a worker claimed a batch of messages.
     *
     * @param observation the worker observation context
     * @param claimedCount the number of claimed messages
     */
    void workerClaimed(WorkerObservation observation, int claimedCount);

    /**
     * Records that a worker processed a single message successfully.
     *
     * @param observation the worker observation context
     */
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

    /**
     * Records that a worker scheduled a message for another retry attempt.
     *
     * @param observation the worker observation context
     */
    void workerRetried(WorkerObservation observation);

    /**
     * Records that a worker stopped retrying a message.
     *
     * @param observation the worker observation context
     */
    void workerStopped(WorkerObservation observation);

    /**
     * Records an administrative redrive action.
     *
     * @param observation the redrive observation context
     * @param redrivenCount the number of messages moved back to READY
     */
    default void workerRedriven(WorkerObservation observation, int redrivenCount) {}

    /**
     * Records that a worker failed unexpectedly while handling a message.
     *
     * @param observation the worker observation context
     * @param error the failure that interrupted processing
     */
    void workerFailed(WorkerObservation observation, Throwable error);

    /**
     * Records that a maintenance routine completed successfully.
     *
     * @param observation the maintenance observation context
     */
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
     * Records queue backlog counts for the default topic.
     *
     * @param mode the worker mode that produced the backlog snapshot
     * @param statusCounts the queue counts grouped by status
     */
    default void queueBacklogObserved(String mode, Map<String, Long> statusCounts) {
        queueBacklogObserved(mode, "default", statusCounts);
    }

    /**
     * Records queue backlog counts grouped by status.
     *
     * @param mode the worker mode that produced the backlog snapshot
     * @param topic the topic whose queue counts were observed
     * @param statusCounts the queue counts grouped by status
     */
    default void queueBacklogObserved(String mode, String topic, Map<String, Long> statusCounts) {}

    /**
     * Records a cleanup maintenance result.
     *
     * @param observation the cleanup observation context
     * @param deletedCount the number of deleted rows
     */
    default void maintenanceCleanup(MaintenanceObservation observation, int deletedCount) {}

    /**
     * Records that a maintenance routine failed.
     *
     * @param observation the maintenance observation context
     * @param error the failure that interrupted the maintenance action
     */
    void maintenanceFailed(MaintenanceObservation observation, Throwable error);
}
