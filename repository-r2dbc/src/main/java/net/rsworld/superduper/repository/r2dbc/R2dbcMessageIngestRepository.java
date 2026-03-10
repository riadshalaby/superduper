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
    public Mono<Void> upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        return upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
    }

    @Override
    public Mono<Void> upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        var statement = db.sql(dialect.upsertReadyMessageSql())
                .bind("topic", topic)
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", content)
                .bind("occurredAt", LocalDateTime.ofInstant(occurredAt, ZoneOffset.UTC));
        statement = bindNullable(statement, "correlationId", correlationId);
        statement = bindNullable(statement, "messageType", messageType);
        return statement.fetch().rowsUpdated().then();
    }

    private DatabaseClient.GenericExecuteSpec bindNullable(
            DatabaseClient.GenericExecuteSpec statement, String name, String value) {
        if (value == null) {
            return statement.bindNull(name, String.class);
        }
        return statement.bind(name, value);
    }
}
