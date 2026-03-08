package net.rsworld.superduper.repository.r2dbc;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class R2dbcWorkerMessageRepository implements ReactiveWorkerMessageRepository {

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final R2dbcSqlDialect dialect;

    public R2dbcWorkerMessageRepository(DatabaseClient db, TransactionalOperator tx, SqlDialect dialect) {
        this(db, tx, R2dbcSqlDialects.from(dialect));
    }

    public R2dbcWorkerMessageRepository(DatabaseClient db, TransactionalOperator tx, R2dbcSqlDialect dialect) {
        this.db = db;
        this.tx = tx;
        this.dialect = dialect;
    }

    @Override
    public Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries) {
        return tx.transactional(db.sql(dialect.claimBatchSql())
                .bind("cid", workerId)
                .bind("batch", batchSize)
                .bind("maxRetries", maxRetries)
                .fetch()
                .rowsUpdated()
                .defaultIfEmpty(0L));
    }

    @Override
    public Flux<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return db.sql(dialect.fetchClaimedForWorkerSql())
                .bind("cid", workerId)
                .map((row, metadata) -> mapClaimedMessage(row))
                .all();
    }

    @Override
    public Flux<ClaimedMessage> findByStatus(String status, int limit) {
        return db.sql(dialect.findByStatusSql())
                .bind("status", status)
                .bind("limit", limit)
                .map((row, metadata) -> mapClaimedMessage(row))
                .all();
    }

    @Override
    public Mono<Integer> redriveById(long id) {
        return db.sql(dialect.redriveByIdSql())
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Integer> redriveByStatus(String status, int limit) {
        return db.sql(dialect.redriveByStatusSql())
                .bind("status", status)
                .bind("limit", limit)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Map<String, Long>> countByStatus() {
        return db.sql(dialect.countByStatusSql())
                .map((row, metadata) -> Map.entry(row.get("status", String.class), row.get("cnt", Long.class)))
                .all()
                .collectList()
                .map(rows -> {
                    Map<String, Long> counts = new LinkedHashMap<>();
                    for (Map.Entry<String, Long> row : rows) {
                        if (row.getKey() != null && row.getValue() != null) {
                            counts.put(row.getKey(), row.getValue());
                        }
                    }
                    return counts;
                });
    }

    private static ClaimedMessage mapClaimedMessage(io.r2dbc.spi.Readable row) {
        Number id = row.get("id", Number.class);
        Number retry = row.get("retry_count", Number.class);
        return new ClaimedMessage(
                id == null ? null : id.longValue(),
                row.get("message_id", String.class),
                row.get("message_key", String.class),
                row.get("content", String.class),
                retry == null ? 0 : retry.intValue(),
                row.get("container_id", String.class),
                row.get("correlation_id", String.class),
                row.get("message_type", String.class));
    }

    @Override
    public Mono<Integer> releaseMessages(java.util.List<Long> ids, String containerId) {
        if (ids == null || ids.isEmpty()) {
            return Mono.just(0);
        }
        return db.sql(dialect.releaseMessagesSql())
                .bind("ids", ids)
                .bind("cid", containerId)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue)
                .defaultIfEmpty(0);
    }

    @Override
    public Mono<Boolean> markProcessed(long id, String containerId) {
        return db.sql(dialect.markProcessedSql())
                .bind("id", id)
                .bind("cid", containerId)
                .fetch()
                .rowsUpdated()
                .map(updated -> updated > 0)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Boolean> markFailed(long id, int retryCount, String containerId) {
        return db.sql(dialect.markFailedSql())
                .bind("id", id)
                .bind("r", retryCount)
                .bind("cid", containerId)
                .fetch()
                .rowsUpdated()
                .map(updated -> updated > 0)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Boolean> markStopped(long id, int retryCount, String containerId) {
        return db.sql(dialect.markStoppedSql())
                .bind("id", id)
                .bind("r", retryCount)
                .bind("cid", containerId)
                .fetch()
                .rowsUpdated()
                .map(updated -> updated > 0)
                .defaultIfEmpty(false);
    }
}
