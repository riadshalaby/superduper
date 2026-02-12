package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ReactiveHeartbeatService {
    private final ReactiveWorkerMaintenanceRepository maintenanceRepository;
    private final SuperduperObserver observer;
    private final String workerId;

    public ReactiveHeartbeatService(
            ReactiveWorkerMaintenanceRepository maintenanceRepository, SuperduperObserver observer) {
        this.maintenanceRepository = maintenanceRepository;
        this.observer = observer;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(
            fixedRateString = "${superduper.worker.heartbeat-interval-ms:30000}",
            initialDelayString = "${superduper.worker.heartbeat-initial-delay-ms:0}")
    public void heartbeat() {
        long started = System.nanoTime();
        maintenanceRepository
                .heartbeat(workerId)
                .doOnSuccess(x -> observer.maintenanceSucceeded(
                        new MaintenanceObservation("reactive", workerId, "heartbeat", elapsedMs(started))))
                .onErrorResume(e -> {
                    observer.maintenanceFailed(
                            new MaintenanceObservation("reactive", workerId, "heartbeat", elapsedMs(started)), e);
                    return reactor.core.publisher.Mono.empty();
                })
                .subscribe();
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
