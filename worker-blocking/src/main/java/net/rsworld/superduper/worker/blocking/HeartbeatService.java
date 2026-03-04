package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class HeartbeatService {
    private final WorkerMaintenanceRepository maintenanceRepository;
    private final SuperduperObserver observer;
    private final String workerId;

    public HeartbeatService(WorkerMaintenanceRepository maintenanceRepository, SuperduperObserver observer) {
        this.maintenanceRepository = maintenanceRepository;
        this.observer = observer;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.heartbeat-interval-ms:30000}",
            initialDelayString = "${superduper.worker.heartbeat-initial-delay-ms:0}")
    public void heartbeat() {
        long started = System.nanoTime();
        try {
            maintenanceRepository.heartbeat(workerId);
            observer.maintenanceSucceeded(
                    new MaintenanceObservation("blocking", workerId, "heartbeat", elapsedMs(started)));
        } catch (RuntimeException e) {
            observer.maintenanceFailed(
                    new MaintenanceObservation("blocking", workerId, "heartbeat", elapsedMs(started)), e);
            throw e;
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
