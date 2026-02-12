package net.rsworld.superduper.repository.r2dbc;

import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

public class R2dbcMessageIngestRepository implements ReactiveMessageIngestRepository {

    private final DatabaseClient db;
    private final String upsertSql;

    public R2dbcMessageIngestRepository(DatabaseClient db, SqlDialect dialect) {
        this.db = db;
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
    public Mono<Void> upsertReadyMessage(String uuid, String key, String content) {
        return db.sql(upsertSql)
                .bind("uuid", uuid)
                .bind("key", key)
                .bind("content", content)
                .fetch()
                .rowsUpdated()
                .then();
    }
}
