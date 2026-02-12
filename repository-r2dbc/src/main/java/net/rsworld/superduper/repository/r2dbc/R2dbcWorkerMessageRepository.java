package net.rsworld.superduper.repository.r2dbc;

import java.util.List;
import java.util.stream.Collectors;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class R2dbcWorkerMessageRepository implements ReactiveWorkerMessageRepository {

    private final DatabaseClient db;
    private final TransactionalOperator tx;

    public R2dbcWorkerMessageRepository(DatabaseClient db, TransactionalOperator tx) {
        this.db = db;
        this.tx = tx;
    }

    @Override
    public Flux<Long> claimBatch(String workerId, int batchSize, int maxRetries) {
        return tx.transactional(db.sql("SELECT m1.id FROM messages m1 "
                        + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                        + "AND NOT EXISTS (SELECT 1 FROM messages p WHERE p.key = m1.key AND p.status = 'PROCESSING') "
                        + "AND NOT EXISTS (SELECT 1 FROM messages prev WHERE prev.key = m1.key "
                        + "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                        + "ORDER BY m1.id LIMIT :batch FOR UPDATE SKIP LOCKED")
                .bind("batch", batchSize)
                .bind("maxRetries", maxRetries)
                .map((row, metadata) -> {
                    Number id = row.get("id", Number.class);
                    return id == null ? null : id.longValue();
                })
                .all()
                .filter(id -> id != null)
                .collectList()
                .flatMapMany(ids -> {
                    if (ids.isEmpty()) {
                        return Flux.empty();
                    }
                    String inClause = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
                    return db.sql("UPDATE messages SET status='PROCESSING', container_id=:cid, last_updated=NOW() "
                                    + "WHERE id IN ("
                                    + inClause
                                    + ") AND (status='READY' OR (status='FAILED' AND retry_count < :maxRetries))")
                            .bind("cid", workerId)
                            .bind("maxRetries", maxRetries)
                            .fetch()
                            .rowsUpdated()
                            .thenMany(Flux.fromIterable(ids));
                }));
    }

    @Override
    public Flux<ClaimedMessage> fetchClaimedByIds(List<Long> ids) {
        if (ids.isEmpty()) {
            return Flux.empty();
        }
        String inClause = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
        return db.sql("SELECT id, key, content, retry_count, container_id FROM messages WHERE id IN (" + inClause
                        + ") ORDER BY key, id")
                .map((row, metadata) -> {
                    Number id = row.get("id", Number.class);
                    Number retry = row.get("retry_count", Number.class);
                    return new ClaimedMessage(
                            id == null ? null : id.longValue(),
                            row.get("key", String.class),
                            row.get("content", String.class),
                            retry == null ? 0 : retry.intValue(),
                            row.get("container_id", String.class));
                })
                .all();
    }

    @Override
    public Mono<Void> markProcessed(long id) {
        return db.sql("UPDATE messages SET status='PROCESSED', last_updated=NOW() WHERE id=:id")
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> markReadyForRetry(long id, int retryCount) {
        return db.sql(
                        "UPDATE messages SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id")
                .bind("id", id)
                .bind("r", retryCount)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> markStopped(long id, int retryCount) {
        return db.sql(
                        "UPDATE messages SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id")
                .bind("id", id)
                .bind("r", retryCount)
                .fetch()
                .rowsUpdated()
                .then();
    }
}
