package net.rsworld.superduper.repository.jdbc;

public final class PostgresJdbcSqlDialect implements JdbcSqlDialect {

    @Override
    public String upsertReadyMessageSql() {
        return "INSERT INTO messages (uuid, key, content, status, occurred_at, received_at, retry_count, last_updated) "
                + "VALUES (:uuid, :key, :content, 'READY', :occurredAt, NOW(), 0, NOW()) "
                + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated";
    }

    @Override
    public String claimBatchSql() {
        return "SELECT m1.id FROM messages m1 "
                + "LEFT JOIN messages p ON p.key = m1.key AND p.status = 'PROCESSING' "
                + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                + "AND p.id IS NULL "
                + "AND NOT EXISTS (SELECT 1 FROM messages prev WHERE prev.key = m1.key "
                + "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                + "ORDER BY m1.id LIMIT :batch FOR UPDATE OF m1 SKIP LOCKED";
    }

    @Override
    public String claimBatchUpdateSql() {
        return "UPDATE messages SET status='PROCESSING', container_id=:cid, last_updated=NOW() "
                + "WHERE id IN (:ids) "
                + "AND (status='READY' OR (status='FAILED' AND retry_count < :maxRetries))";
    }

    @Override
    public String fetchClaimedSql() {
        return "SELECT id, key AS message_key, content, retry_count, container_id "
                + "FROM messages WHERE id IN (:ids) ORDER BY key, id";
    }

    @Override
    public String markProcessedSql() {
        return "UPDATE messages SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() WHERE id=:id";
    }

    @Override
    public String markReadyForRetrySql() {
        return "UPDATE messages SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id";
    }

    @Override
    public String markStoppedSql() {
        return "UPDATE messages SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id";
    }

    @Override
    public String reclaimStaleProcessingSql() {
        return "UPDATE messages SET status='READY', container_id=NULL "
                + "WHERE status='PROCESSING' AND last_updated < (NOW() - (:t * INTERVAL '1 second'))";
    }

    @Override
    public String reclaimMissingHeartbeatsSql() {
        return "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' "
                + "AND (container_id IS NULL OR container_id NOT IN ("
                + "SELECT container_id FROM container_heartbeats "
                + "WHERE last_heartbeat >= (NOW() - (:hb * INTERVAL '1 second'))))";
    }

    @Override
    public String heartbeatUpsertSql() {
        return "INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat";
    }
}
