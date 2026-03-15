package net.rsworld.superduper.repository.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindMarkersFactoryResolver;
import org.springframework.r2dbc.core.binding.BindTarget;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class R2dbcMessageIngestRepository implements ReactiveMessageIngestRepository {

    private final DatabaseClient db;
    private final R2dbcSqlDialect dialect;
    private final BatchStatementTemplate batchStatementTemplate;

    public R2dbcMessageIngestRepository(DatabaseClient db, SqlDialect dialect) {
        this(db, R2dbcSqlDialects.from(dialect));
    }

    public R2dbcMessageIngestRepository(DatabaseClient db, R2dbcSqlDialect dialect) {
        this.db = db;
        this.dialect = dialect;
        this.batchStatementTemplate = BatchStatementTemplate.create(db.getConnectionFactory(), dialect);
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

    @Override
    public Mono<Void> batchUpsertReadyMessages(List<MessageIngestData> messages) {
        Objects.requireNonNull(messages, "messages must not be null");
        if (messages.isEmpty()) {
            return Mono.empty();
        }
        return db.inConnectionMany(connection -> {
                    Statement statement = connection.createStatement(batchStatementTemplate.sql());
                    boolean first = true;
                    for (MessageIngestData message : messages) {
                        if (!first) {
                            statement.add();
                        }
                        batchStatementTemplate.bind(statement, message);
                        first = false;
                    }
                    return Flux.from(statement.execute()).flatMap(result -> Flux.from(result.getRowsUpdated()));
                })
                .then();
    }

    private DatabaseClient.GenericExecuteSpec bindNullable(
            DatabaseClient.GenericExecuteSpec statement, String name, String value) {
        if (value == null) {
            return statement.bindNull(name, String.class);
        }
        return statement.bind(name, value);
    }

    private record BatchStatementTemplate(String sql, List<ParameterBinding> bindings) {
        static BatchStatementTemplate create(ConnectionFactory connectionFactory, R2dbcSqlDialect dialect) {
            BindMarkers bindMarkers =
                    BindMarkersFactoryResolver.resolve(connectionFactory).create();
            String expandedSql = dialect.upsertReadyMessageSql();
            List<ParameterBinding> bindings = new ArrayList<>(List.of(
                    parameter("topic", String.class, MessageIngestData::topic),
                    parameter("messageId", String.class, MessageIngestData::messageId),
                    parameter("messageKey", String.class, MessageIngestData::messageKey),
                    parameter("content", String.class, MessageIngestData::content),
                    parameter(
                            "occurredAt",
                            LocalDateTime.class,
                            message -> LocalDateTime.ofInstant(message.occurredAt(), ZoneOffset.UTC)),
                    parameter("correlationId", String.class, MessageIngestData::correlationId),
                    parameter("messageType", String.class, MessageIngestData::messageType)));
            for (int index = 0; index < bindings.size(); index++) {
                ParameterBinding binding = bindings.get(index);
                BindMarker marker = bindMarkers.next(binding.name());
                bindings.set(index, binding.withMarker(marker));
                expandedSql = expandedSql.replace(":" + binding.name(), marker.getPlaceholder());
            }
            return new BatchStatementTemplate(expandedSql, List.copyOf(bindings));
        }

        void bind(Statement statement, MessageIngestData message) {
            StatementBindTarget target = new StatementBindTarget(statement);
            for (ParameterBinding binding : bindings) {
                Object value = binding.extractor().apply(message);
                if (value == null) {
                    binding.marker().bindNull(target, binding.nullType());
                } else {
                    binding.marker().bind(target, value);
                }
            }
        }

        private static ParameterBinding parameter(
                String name, Class<?> nullType, Function<MessageIngestData, Object> extractor) {
            return new ParameterBinding(name, null, nullType, extractor);
        }
    }

    private record ParameterBinding(
            String name, BindMarker marker, Class<?> nullType, Function<MessageIngestData, Object> extractor) {
        ParameterBinding withMarker(BindMarker bindMarker) {
            return new ParameterBinding(name, bindMarker, nullType, extractor);
        }
    }

    private static final class StatementBindTarget implements BindTarget {
        private final Statement statement;

        private StatementBindTarget(Statement statement) {
            this.statement = statement;
        }

        @Override
        public void bind(String identifier, Object value) {
            statement.bind(identifier, value);
        }

        @Override
        public void bind(int index, Object value) {
            statement.bind(index, value);
        }

        @Override
        public void bindNull(String identifier, Class<?> type) {
            statement.bindNull(identifier, type);
        }

        @Override
        public void bindNull(int index, Class<?> type) {
            statement.bindNull(index, type);
        }
    }
}
