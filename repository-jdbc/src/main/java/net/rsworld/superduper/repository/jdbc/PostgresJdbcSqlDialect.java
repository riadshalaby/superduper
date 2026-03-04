package net.rsworld.superduper.repository.jdbc;

public final class PostgresJdbcSqlDialect implements JdbcSqlDialect {
    private final String messagesTable;
    private final String heartbeatsTable;

    public PostgresJdbcSqlDialect() {
        this("messages", "container_heartbeats");
    }

    public PostgresJdbcSqlDialect(String messagesTable, String heartbeatsTable) {
        this.messagesTable = messagesTable;
        this.heartbeatsTable = heartbeatsTable;
    }

    @Override
    public String upsertReadyMessageSql() {
        return ("INSERT INTO %s (uuid, key, content, status, occurred_at, received_at, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', :occurredAt, NOW(), 0, NOW()) "
                        + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated")
                .formatted(messagesTable);
    }

    @Override
    public String claimBatchSql() {
        return ("WITH candidate AS ("
                        + "SELECT m1.id FROM %s m1 "
                        + "LEFT JOIN %s p ON p.key = m1.key AND p.status = 'PROCESSING' "
                        + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                        + "AND p.id IS NULL "
                        + "AND NOT EXISTS (SELECT 1 FROM %s prev WHERE prev.key = m1.key "
                        + "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                        + "ORDER BY m1.id LIMIT :batch FOR UPDATE OF m1 SKIP LOCKED"
                        + ") "
                        + "UPDATE %s m SET status='PROCESSING', container_id=:cid, last_updated=NOW() "
                        + "FROM candidate c WHERE m.id = c.id")
                .formatted(messagesTable, messagesTable, messagesTable, messagesTable);
    }

    @Override
    public String fetchClaimedForWorkerSql() {
        return "SELECT id, key AS message_key, content, retry_count, container_id "
                + "FROM %s WHERE status='PROCESSING' AND container_id=:cid ORDER BY key, id".formatted(messagesTable);
    }

    @Override
    public String markProcessedSql() {
        return ("UPDATE %s SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() "
                        + "WHERE id=:id AND container_id=:cid")
                .formatted(messagesTable);
    }

    @Override
    public String markFailedSql() {
        return ("UPDATE %s SET status='FAILED', retry_count=:r, container_id=NULL, last_updated=NOW() "
                        + "WHERE id=:id AND container_id=:cid")
                .formatted(messagesTable);
    }

    @Override
    public String markStoppedSql() {
        return ("UPDATE %s SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() "
                        + "WHERE id=:id AND container_id=:cid")
                .formatted(messagesTable);
    }

    @Override
    public String reclaimStaleProcessingSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() "
                        + "WHERE status='PROCESSING' AND last_updated < (NOW() - (:t * INTERVAL '1 second'))")
                .formatted(messagesTable);
    }

    @Override
    public String reclaimMissingHeartbeatsSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() WHERE status='PROCESSING' "
                        + "AND (container_id IS NULL OR container_id NOT IN ("
                        + "SELECT container_id FROM %s "
                        + "WHERE last_heartbeat >= (NOW() - (:hb * INTERVAL '1 second'))))")
                .formatted(messagesTable, heartbeatsTable);
    }

    @Override
    public String heartbeatUpsertSql() {
        return ("INSERT INTO %s (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                        + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat")
                .formatted(heartbeatsTable);
    }
}
