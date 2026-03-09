package net.rsworld.superduper.worker.blocking;

import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.scheduling.annotation.Scheduled;

public class CleanupService {
    private static final String MODE_BLOCKING = "blocking";
    private static final String WORKER_ID = "n/a";

    private final WorkerMaintenanceRepository repository;
    private final SuperduperObserver observer;
    private final int processedRetentionDays;
    private final int stoppedRetentionDays;
    private final int heartbeatRetentionDays;

    public CleanupService(
            WorkerMaintenanceRepository repository,
            SuperduperObserver observer,
            int processedRetentionDays,
            int stoppedRetentionDays,
            int heartbeatRetentionDays) {
        this.repository = repository;
        this.observer = observer;
        this.processedRetentionDays = processedRetentionDays;
        this.stoppedRetentionDays = stoppedRetentionDays;
        this.heartbeatRetentionDays = heartbeatRetentionDays;
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.retention.interval-ms:86400000}",
            initialDelayString = "${superduper.worker.retention.initial-delay-ms:60000}")
    public void cleanup() {
        long started = System.nanoTime();
        String operation = "cleanup-processed";
        try {
            int deletedProcessed = repository.deleteProcessedOlderThan(processedRetentionDays);
            operation = "cleanup-stopped";
            int deletedStopped = repository.deleteStoppedOlderThan(stoppedRetentionDays);
            operation = "cleanup-heartbeats";
            int deletedHeartbeats = repository.deleteStaleHeartbeats(heartbeatRetentionDays);
            long durationMs = elapsedMs(started);
            observer.maintenanceCleanup(
                    new MaintenanceObservation(MODE_BLOCKING, WORKER_ID, "cleanup-processed", durationMs),
                    deletedProcessed);
            observer.maintenanceCleanup(
                    new MaintenanceObservation(MODE_BLOCKING, WORKER_ID, "cleanup-stopped", durationMs),
                    deletedStopped);
            observer.maintenanceCleanup(
                    new MaintenanceObservation(MODE_BLOCKING, WORKER_ID, "cleanup-heartbeats", durationMs),
                    deletedHeartbeats);
        } catch (RuntimeException error) {
            observer.maintenanceFailed(
                    new MaintenanceObservation(MODE_BLOCKING, WORKER_ID, operation, elapsedMs(started)), error);
            throw error;
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
