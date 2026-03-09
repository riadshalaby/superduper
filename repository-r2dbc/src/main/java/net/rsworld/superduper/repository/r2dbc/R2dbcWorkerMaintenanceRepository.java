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
        return db.sql(dialect.reclaimStaleProcessingSql())
                .bind("t", orphanTimeoutSec)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return db.sql(dialect.reclaimMissingHeartbeatsSql())
                .bind("hb", heartbeatWindowSec)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> deleteProcessedOlderThan(int retentionDays) {
        return db.sql(dialect.deleteProcessedOlderThanSql())
                .bind("retentionDays", retentionDays)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> deleteStoppedOlderThan(int retentionDays) {
        return db.sql(dialect.deleteStoppedOlderThanSql())
                .bind("retentionDays", retentionDays)
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
