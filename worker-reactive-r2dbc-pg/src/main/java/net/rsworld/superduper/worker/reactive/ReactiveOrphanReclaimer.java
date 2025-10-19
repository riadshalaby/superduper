package net.rsworld.superduper.worker.reactive;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class ReactiveOrphanReclaimer {
    private final DatabaseClient db;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public ReactiveOrphanReclaimer(
            DatabaseClient db,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") int hbMs) {
        this.db = db;
        this.orphanTimeoutSec = orphanTimeoutMs / 1000;
        this.heartbeatWindowSec = hbMs / 1000;
    }

    @Scheduled(fixedDelayString = "${superduper.worker.orphan-timeout-ms:120000}", initialDelay = 15000)
    public void reclaim() {
        Flux.concat(
                        db.sql(
                                        "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' AND last_updated < (NOW() - INTERVAL '"
                                                + orphanTimeoutSec + " SECOND')")
                                .fetch()
                                .rowsUpdated(),
                        db.sql(
                                        "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' AND (container_id IS NULL OR container_id NOT IN ("
                                                + "SELECT container_id FROM container_heartbeats WHERE last_heartbeat >= (NOW() - INTERVAL '"
                                                + heartbeatWindowSec + " SECOND')))")
                                .fetch()
                                .rowsUpdated())
                .onErrorResume(e -> Flux.empty())
                .subscribe();
    }
}
