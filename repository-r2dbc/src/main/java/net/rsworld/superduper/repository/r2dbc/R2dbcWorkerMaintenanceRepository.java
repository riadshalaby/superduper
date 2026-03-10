package net.rsworld.superduper.repository.r2dbc;

import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

public class R2dbcWorkerMaintenanceRepository implements ReactiveWorkerMaintenanceRepository {

    private final DatabaseClient db;
    private final R2dbcSqlDialect dialect;

    public R2dbcWorkerMaintenanceRepository(DatabaseClient db, R2dbcSqlDialect dialect) {
        this.db = db;
        this.dialect = dialect;
    }

    @Override
    public Mono<Void> heartbeat(String workerId) {
        return db.sql(dialect.heartbeatUpsertSql())
                .bind("cid", workerId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec) {
        return reclaimStaleProcessing(orphanTimeoutSec, "default");
    }

    @Override
    public Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec, String topic) {
        return db.sql(dialect.reclaimStaleProcessingSql())
                .bind("t", orphanTimeoutSec)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return reclaimMissingHeartbeats(heartbeatWindowSec, "default");
    }

    @Override
    public Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec, String topic) {
        return db.sql(dialect.reclaimMissingHeartbeatsSql())
                .bind("hb", heartbeatWindowSec)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> deleteProcessedOlderThan(int retentionDays) {
        return deleteProcessedOlderThan(retentionDays, "default");
    }

    @Override
    public Mono<Integer> deleteProcessedOlderThan(int retentionDays, String topic) {
        return db.sql(dialect.deleteProcessedOlderThanSql())
                .bind("retentionDays", retentionDays)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> deleteStoppedOlderThan(int retentionDays) {
        return deleteStoppedOlderThan(retentionDays, "default");
    }

    @Override
    public Mono<Integer> deleteStoppedOlderThan(int retentionDays, String topic) {
        return db.sql(dialect.deleteStoppedOlderThanSql())
                .bind("retentionDays", retentionDays)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> deleteStaleHeartbeats(int retentionDays) {
        return db.sql(dialect.deleteStaleHeartbeatsSql())
                .bind("retentionDays", retentionDays)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }
}
