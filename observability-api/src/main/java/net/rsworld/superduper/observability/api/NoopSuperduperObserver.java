package net.rsworld.superduper.observability.api;

import java.util.Map;

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
    public void workerBatchCompleted(WorkerObservation observation, int processed, int failed, int stopped) {
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
    public void workerRedriven(WorkerObservation observation, int redrivenCount) {
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
    public void maintenanceSucceeded(MaintenanceObservation observation, int reclaimedCount) {
        // Intentionally no-op.
    }

    @Override
    public void queueBacklogObserved(String mode, Map<String, Long> statusCounts) {
        // Intentionally no-op.
    }

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {
        // Intentionally no-op.
    }
}
