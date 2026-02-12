package net.rsworld.superduper.repository.jdbc;

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
                "INSERT INTO messages (uuid, key, content, status, timestamp, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', NOW(), 0, NOW()) "
                        + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated";
            case MARIADB ->
                "INSERT INTO messages (uuid, `key`, content, status, timestamp, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', NOW(), 0, NOW()) "
                        + "ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)";
        };
    }

    @Override
    public void upsertReadyMessage(String uuid, String key, String content) {
        jdbc.update(
                upsertSql,
                new MapSqlParameterSource()
                        .addValue("uuid", uuid)
                        .addValue("key", key)
                        .addValue("content", content));
    }
}
