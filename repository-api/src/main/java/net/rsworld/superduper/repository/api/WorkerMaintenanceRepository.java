package net.rsworld.superduper.repository.api;

public interface WorkerMaintenanceRepository {
    void heartbeat(String workerId);

    void reclaimStaleProcessing(int orphanTimeoutSec);

    void reclaimMissingHeartbeats(int heartbeatWindowSec);
}
