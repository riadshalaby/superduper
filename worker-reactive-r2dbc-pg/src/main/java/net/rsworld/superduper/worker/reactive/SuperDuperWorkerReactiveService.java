package net.rsworld.superduper.worker.reactive;

import jakarta.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class SuperDuperWorkerReactiveService {

    private final DatabaseClient db;
    private final ReactiveMessageHandler handler;
    private final String workerId;
    private final int batchSize;
    private final int maxRetries;
    private final long claimIntervalMs;
    private final long heartbeatIntervalMs;
    private final long orphanIntervalMs;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public SuperDuperWorkerReactiveService(
            DatabaseClient db,
            ReactiveMessageHandler handler,
            @Value("${superduper.worker.batch-size:100}") int batchSize,
            @Value("${superduper.worker.max-retries:5}") int maxRetries,
            @Value("${superduper.worker.claim-interval-ms:5000}") long claimIntervalMs,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") long heartbeatIntervalMs,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") long orphanTimeoutMs) {
        this.db = db;
        this.handler = handler;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.claimIntervalMs = claimIntervalMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.orphanIntervalMs = orphanTimeoutMs;
        this.orphanTimeoutSec = (int) (orphanTimeoutMs / 1000);
        this.heartbeatWindowSec = (int) (heartbeatIntervalMs / 1000);
        this.workerId = ManagementFactory.getRuntimeMXBean().getName() + ":" + UUID.randomUUID();
    }

    @PostConstruct
    public void startLoops() {
        Flux.interval(Duration.ofMillis(claimIntervalMs))
                .concatMap(tick -> claimBatch()
                        .collectList()
                        .flatMapMany(this::fetchClaimed)
                        .concatMap(this::processOne))
                .subscribe();

        Flux.interval(Duration.ofMillis(heartbeatIntervalMs))
                .flatMap(t -> db.sql(
                                "INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                                        + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat")
                        .bind("cid", workerId)
                        .fetch()
                        .rowsUpdated())
                .subscribe();

        Flux.interval(Duration.ofMillis(orphanIntervalMs))
                .flatMap(t -> Flux.concat(
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
                                .rowsUpdated()))
                .subscribe();
    }

    private Flux<Integer> claimBatch() {
        String sql = """
            WITH candidate AS (
                SELECT m1.id
                FROM messages m1
                LEFT JOIN messages p ON p.key = m1.key AND p.status = 'PROCESSING'
                WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
                  AND p.id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM messages prev
                      WHERE prev.key = m1.key
                        AND prev.id < m1.id
                        AND prev.status IN ('READY','FAILED','PROCESSING')
                  )
                ORDER BY m1.id
                LIMIT :batch
            )
            UPDATE messages m
               SET status='PROCESSING', container_id=:cid, last_updated=NOW()
              FROM candidate c
             WHERE m.id = c.id
            RETURNING m.id
        """;
        return db.sql(sql)
                .bind("batch", batchSize)
                .bind("maxRetries", maxRetries)
                .bind("cid", workerId)
                .map(row -> row.get("id", Integer.class))
                .all();
    }

    private Flux<Map<String, Object>> fetchClaimed(List<Integer> ids) {
        if (ids.isEmpty()) return Flux.empty();
        String inClause = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
        return db.sql("SELECT id, key, content, retry_count FROM messages WHERE id IN (" + inClause
                        + ") ORDER BY key, id")
                .fetch()
                .all();
    }

    private Mono<Void> processOne(Map<String, Object> row) {
        int id = ((Number) row.get("id")).intValue();
        int retry = row.get("retry_count") == null ? 0 : ((Number) row.get("retry_count")).intValue();
        MessageRow mr = new MessageRow(
                id, null, (String) row.get("key"), (String) row.get("content"), "PROCESSING", retry, workerId);
        return handler.handle(mr).flatMap(result -> {
            if (result == ProcessingResult.SUCCESS) {
                return db.sql("UPDATE messages SET status='PROCESSED', last_updated=NOW() WHERE id=:id")
                        .bind("id", id)
                        .fetch()
                        .rowsUpdated()
                        .then();
            } else {
                int next = retry + 1;
                if (next < maxRetries) {
                    return db.sql(
                                    "UPDATE messages SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id")
                            .bind("r", next)
                            .bind("id", id)
                            .fetch()
                            .rowsUpdated()
                            .then();
                } else {
                    return db.sql(
                                    "UPDATE messages SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id")
                            .bind("r", next)
                            .bind("id", id)
                            .fetch()
                            .rowsUpdated()
                            .then();
                }
            }
        });
    }
}
