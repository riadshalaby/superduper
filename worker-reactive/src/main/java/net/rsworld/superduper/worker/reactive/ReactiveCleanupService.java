package net.rsworld.superduper.worker.reactive;

import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

public class ReactiveCleanupService {
    private static final String MODE_REACTIVE = "reactive";
    private static final String WORKER_ID = "n/a";

    private final ReactiveWorkerMaintenanceRepository repository;
    private final SuperduperObserver observer;
    private final int processedRetentionDays;
    private final int stoppedRetentionDays;
    private final int heartbeatRetentionDays;

    public ReactiveCleanupService(
            ReactiveWorkerMaintenanceRepository repository,
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
        repository
                .deleteProcessedOlderThan(processedRetentionDays)
                .doOnError(error -> emitFailure(started, "cleanup-processed", error))
                .flatMap(deletedProcessed -> repository
                        .deleteStoppedOlderThan(stoppedRetentionDays)
                        .doOnError(error -> emitFailure(started, "cleanup-stopped", error))
                        .flatMap(deletedStopped -> repository
                                .deleteStaleHeartbeats(heartbeatRetentionDays)
                                .doOnError(error -> emitFailure(started, "cleanup-heartbeats", error))
                                .doOnNext(deletedHeartbeats ->
                                        emitCleanup(started, deletedProcessed, deletedStopped, deletedHeartbeats))))
                .onErrorResume(error -> Mono.empty())
                .block();
    }

    private void emitCleanup(long started, int deletedProcessed, int deletedStopped, int deletedHeartbeats) {
        long durationMs = elapsedMs(started);
        observer.maintenanceCleanup(
                new MaintenanceObservation(MODE_REACTIVE, WORKER_ID, "cleanup-processed", durationMs),
                deletedProcessed);
        observer.maintenanceCleanup(
                new MaintenanceObservation(MODE_REACTIVE, WORKER_ID, "cleanup-stopped", durationMs), deletedStopped);
        observer.maintenanceCleanup(
                new MaintenanceObservation(MODE_REACTIVE, WORKER_ID, "cleanup-heartbeats", durationMs),
                deletedHeartbeats);
    }

    private void emitFailure(long started, String operation, Throwable error) {
        observer.maintenanceFailed(
                new MaintenanceObservation(MODE_REACTIVE, WORKER_ID, operation, elapsedMs(started)), error);
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
