package net.rsworld.superduper.repository.r2dbc;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

public class R2dbcMessageIngestRepository implements ReactiveMessageIngestRepository {

    private final DatabaseClient db;
    private final R2dbcSqlDialect dialect;

    public R2dbcMessageIngestRepository(DatabaseClient db, SqlDialect dialect) {
        this(db, R2dbcSqlDialects.from(dialect));
    }

    public R2dbcMessageIngestRepository(DatabaseClient db, R2dbcSqlDialect dialect) {
        this.db = db;
        this.dialect = dialect;
    }

    @Override
    public Mono<Void> upsertReadyMessage(String uuid, String key, String content, Instant occurredAt) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        return db.sql(dialect.upsertReadyMessageSql())
                .bind("uuid", uuid)
                .bind("key", key)
                .bind("content", content)
                .bind("occurredAt", LocalDateTime.ofInstant(occurredAt, ZoneOffset.UTC))
                .fetch()
                .rowsUpdated()
                .then();
    }
}
