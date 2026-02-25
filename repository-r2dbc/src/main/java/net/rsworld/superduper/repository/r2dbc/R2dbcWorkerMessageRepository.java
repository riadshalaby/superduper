package net.rsworld.superduper.repository.r2dbc;

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
                .map((row, metadata) -> {
                    Number id = row.get("id", Number.class);
                    Number retry = row.get("retry_count", Number.class);
                    return new ClaimedMessage(
                            id == null ? null : id.longValue(),
                            row.get("message_key", String.class),
                            row.get("content", String.class),
                            retry == null ? 0 : retry.intValue(),
                            row.get("container_id", String.class));
                })
                .all();
    }

    @Override
    public Mono<Void> markProcessed(long id) {
        return db.sql(dialect.markProcessedSql())
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> markReadyForRetry(long id, int retryCount) {
        return db.sql(dialect.markReadyForRetrySql())
                .bind("id", id)
                .bind("r", retryCount)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> markStopped(long id, int retryCount) {
        return db.sql(dialect.markStoppedSql())
                .bind("id", id)
                .bind("r", retryCount)
                .fetch()
                .rowsUpdated()
                .then();
    }
}
