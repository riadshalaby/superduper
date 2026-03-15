package net.rsworld.superduper.repository.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class JdbcMessageIngestRepository implements MessageIngestRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcSqlDialect dialect;

    public JdbcMessageIngestRepository(NamedParameterJdbcTemplate jdbc, SqlDialect dialect) {
        this(jdbc, JdbcSqlDialects.from(dialect));
    }

    public JdbcMessageIngestRepository(NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    @Override
    public void upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
    }

    @Override
    public void upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        jdbc.update(
                dialect.upsertReadyMessageSql(),
                toParameterSource(new MessageIngestData(
                        topic, messageId, messageKey, content, occurredAt, correlationId, messageType)));
    }

    @Override
    public void batchUpsertReadyMessages(List<MessageIngestData> messages) {
        Objects.requireNonNull(messages, "messages must not be null");
        if (messages.isEmpty()) {
            return;
        }
        SqlParameterSource[] batchParameters =
                messages.stream().map(this::toParameterSource).toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(dialect.upsertReadyMessageSql(), batchParameters);
    }

    private MapSqlParameterSource toParameterSource(MessageIngestData message) {
        Objects.requireNonNull(message.occurredAt(), "occurredAt must not be null");
        return new MapSqlParameterSource()
                .addValue("topic", message.topic())
                .addValue("messageId", message.messageId())
                .addValue("messageKey", message.messageKey())
                .addValue("content", message.content())
                .addValue("occurredAt", Timestamp.from(message.occurredAt()))
                .addValue("correlationId", message.correlationId())
                .addValue("messageType", message.messageType());
    }
}
