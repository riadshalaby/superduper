package net.rsworld.superduper.repository.api;

public interface WorkerMaintenanceRepository {
    void heartbeat(String workerId);

    int reclaimStaleProcessing(int orphanTimeoutSec);

    int reclaimMissingHeartbeats(int heartbeatWindowSec);
}
