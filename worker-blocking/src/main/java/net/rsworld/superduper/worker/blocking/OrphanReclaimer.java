package net.rsworld.superduper.worker.blocking;

import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OrphanReclaimer {
    private final WorkerMaintenanceRepository maintenanceRepository;
    private final SuperduperObserver observer;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public OrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
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
        try {
            int reclaimedCount = maintenanceRepository.reclaimStaleProcessing(orphanTimeoutSec)
                    + maintenanceRepository.reclaimMissingHeartbeats(heartbeatWindowSec);
            observer.maintenanceSucceeded(
                    new MaintenanceObservation("blocking", "n/a", "orphan-reclaim", elapsedMs(started)),
                    reclaimedCount);
        } catch (RuntimeException e) {
            observer.maintenanceFailed(
                    new MaintenanceObservation("blocking", "n/a", "orphan-reclaim", elapsedMs(started)), e);
            throw e;
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
