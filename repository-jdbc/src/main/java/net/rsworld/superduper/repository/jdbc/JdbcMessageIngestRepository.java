package net.rsworld.superduper.repository.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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
    public void upsertReadyMessage(String uuid, String key, String content, Instant occurredAt) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        jdbc.update(
                dialect.upsertReadyMessageSql(),
                new MapSqlParameterSource()
                        .addValue("uuid", uuid)
                        .addValue("key", key)
                        .addValue("content", content)
                        .addValue("occurredAt", Timestamp.from(occurredAt)));
    }
}
