package net.rsworld.superduper.worker.reactive;

import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ReactiveOrphanReclaimer {
    private final ReactiveWorkerMaintenanceRepository maintenanceRepository;
    private final SuperduperObserver observer;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public ReactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-window-ms:90000}") int heartbeatWindowMs) {
        this.maintenanceRepository = maintenanceRepository;
        this.observer = observer;
        this.orphanTimeoutSec = orphanTimeoutMs / 1000;
        this.heartbeatWindowSec = heartbeatWindowMs / 1000;
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.orphan-timeout-ms:120000}",
            initialDelayString = "${superduper.worker.orphan-initial-delay-ms:15000}")
    public void reclaim() {
        long started = System.nanoTime();
        Mono.zip(
                        maintenanceRepository.reclaimStaleProcessing(orphanTimeoutSec),
                        maintenanceRepository.reclaimMissingHeartbeats(heartbeatWindowSec))
                .doOnSuccess(counts -> observer.maintenanceSucceeded(
                        new MaintenanceObservation("reactive", "n/a", "orphan-reclaim", elapsedMs(started)),
                        counts.getT1() + counts.getT2()))
                .onErrorResume(e -> {
                    observer.maintenanceFailed(
                            new MaintenanceObservation("reactive", "n/a", "orphan-reclaim", elapsedMs(started)), e);
                    return Mono.empty();
                })
                .block();
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
