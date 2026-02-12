package net.rsworld.superduper.observability.api;

public interface SuperduperObserver {

    void consumerReceived(ConsumerObservation observation);

    void consumerSucceeded(ConsumerObservation observation);

    void consumerFailed(ConsumerObservation observation, Throwable error);

    void workerClaimed(WorkerObservation observation, int claimedCount);

    void workerProcessed(WorkerObservation observation);

    void workerRetried(WorkerObservation observation);

    void workerStopped(WorkerObservation observation);

    void workerFailed(WorkerObservation observation, Throwable error);

    void maintenanceSucceeded(MaintenanceObservation observation);

    void maintenanceFailed(MaintenanceObservation observation, Throwable error);
}
