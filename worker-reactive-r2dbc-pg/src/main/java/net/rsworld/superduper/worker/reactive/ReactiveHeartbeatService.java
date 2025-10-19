package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ReactiveHeartbeatService {
    private final DatabaseClient db;
    private final String workerId;

    public ReactiveHeartbeatService(
            DatabaseClient db, @Value("${superduper.worker.heartbeat-interval-ms:30000}") long hb) {
        this.db = db;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(fixedRateString = "${superduper.worker.heartbeat-interval-ms:30000}")
    public void heartbeat() {
        db.sql("INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                        + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat")
                .bind("cid", workerId)
                .fetch()
                .rowsUpdated()
                .onErrorResume(e -> Mono.just(0L))
                .subscribe();
    }
}
