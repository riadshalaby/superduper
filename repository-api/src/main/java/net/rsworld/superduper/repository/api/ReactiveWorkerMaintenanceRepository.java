package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Mono;

public interface ReactiveWorkerMaintenanceRepository {
    Mono<Void> heartbeat(String workerId);

    Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec);

    Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec);
}
