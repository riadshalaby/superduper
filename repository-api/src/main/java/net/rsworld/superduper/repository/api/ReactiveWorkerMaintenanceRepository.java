package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Mono;

public interface ReactiveWorkerMaintenanceRepository {
    Mono<Void> heartbeat(String workerId);

    Mono<Void> reclaimStaleProcessing(int orphanTimeoutSec);

    Mono<Void> reclaimMissingHeartbeats(int heartbeatWindowSec);
}
