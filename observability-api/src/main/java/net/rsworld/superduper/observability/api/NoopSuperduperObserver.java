package net.rsworld.superduper.observability.api;

public final class NoopSuperduperObserver implements SuperduperObserver {
    public static final NoopSuperduperObserver INSTANCE = new NoopSuperduperObserver();

    private NoopSuperduperObserver() {}

    @Override
    public void consumerReceived(ConsumerObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void consumerSucceeded(ConsumerObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void consumerFailed(ConsumerObservation observation, Throwable error) {
        // Intentionally no-op.
    }

    @Override
    public void workerClaimed(WorkerObservation observation, int claimedCount) {
        // Intentionally no-op.
    }

    @Override
    public void workerProcessed(WorkerObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void workerRetried(WorkerObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void workerStopped(WorkerObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void workerFailed(WorkerObservation observation, Throwable error) {
        // Intentionally no-op.
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation) {
        // Intentionally no-op.
    }

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {
        // Intentionally no-op.
    }
}
