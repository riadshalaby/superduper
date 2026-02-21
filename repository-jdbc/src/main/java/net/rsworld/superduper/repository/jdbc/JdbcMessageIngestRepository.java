package net.rsworld.superduper.repository.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class JdbcMessageIngestRepository implements MessageIngestRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final String upsertSql;

    public JdbcMessageIngestRepository(NamedParameterJdbcTemplate jdbc, SqlDialect dialect) {
        this.jdbc = jdbc;
        this.upsertSql = switch (dialect) {
            case POSTGRES ->
                "INSERT INTO messages (uuid, key, content, status, occurred_at, received_at, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', :occurredAt, NOW(), 0, NOW()) "
                        + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated";
            case MARIADB ->
                "INSERT INTO messages (uuid, `key`, content, status, occurred_at, received_at, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', :occurredAt, NOW(), 0, NOW()) "
                        + "ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)";
        };
    }

    @Override
    public void upsertReadyMessage(String uuid, String key, String content, Instant occurredAt) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        jdbc.update(
                upsertSql,
                new MapSqlParameterSource()
                        .addValue("uuid", uuid)
                        .addValue("key", key)
                        .addValue("content", content)
                        .addValue("occurredAt", Timestamp.from(occurredAt)));
    }
}
