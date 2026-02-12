package net.rsworld.superduper.observability.api;

public final class NoopSuperduperObserver implements SuperduperObserver {
    public static final NoopSuperduperObserver INSTANCE = new NoopSuperduperObserver();

    private NoopSuperduperObserver() {}

    @Override
    public void consumerReceived(ConsumerObservation observation) {}

    @Override
    public void consumerSucceeded(ConsumerObservation observation) {}

    @Override
    public void consumerFailed(ConsumerObservation observation, Throwable error) {}

    @Override
    public void workerClaimed(WorkerObservation observation, int claimedCount) {}

    @Override
    public void workerProcessed(WorkerObservation observation) {}

    @Override
    public void workerRetried(WorkerObservation observation) {}

    @Override
    public void workerStopped(WorkerObservation observation) {}

    @Override
    public void workerFailed(WorkerObservation observation, Throwable error) {}

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation) {}

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {}
}
