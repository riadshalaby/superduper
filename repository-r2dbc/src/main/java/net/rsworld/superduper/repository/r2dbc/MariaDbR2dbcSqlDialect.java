package net.rsworld.superduper.repository.r2dbc;

public final class MariaDbR2dbcSqlDialect implements R2dbcSqlDialect {
    private final String messagesTable;
    private final String heartbeatsTable;

    public MariaDbR2dbcSqlDialect() {
        this("messages", "container_heartbeats");
    }

    public MariaDbR2dbcSqlDialect(String messagesTable, String heartbeatsTable) {
        this.messagesTable = messagesTable;
        this.heartbeatsTable = heartbeatsTable;
    }

    @Override
    public String upsertReadyMessageSql() {
        return ("INSERT INTO %s (uuid, `key`, content, status, occurred_at, received_at, retry_count, last_updated) "
                        + "VALUES (:uuid, :key, :content, 'READY', :occurredAt, NOW(), 0, NOW()) "
                        + "ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)")
                .formatted(messagesTable);
    }

    @Override
    public String claimBatchSql() {
        return ("SELECT m1.id FROM %s m1 "
                        + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                        + "AND NOT EXISTS (SELECT 1 FROM %s p WHERE p.`key` = m1.`key` AND p.status = 'PROCESSING') "
                        + "AND NOT EXISTS (SELECT 1 FROM %s prev WHERE prev.`key` = m1.`key` "
                        + "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                        + "ORDER BY m1.id LIMIT :batch FOR UPDATE SKIP LOCKED")
                .formatted(messagesTable, messagesTable, messagesTable);
    }

    @Override
    public String claimBatchUpdateSqlTemplate() {
        return ("UPDATE %s SET status='PROCESSING', container_id=:cid, last_updated=NOW() "
                        + "WHERE id IN (%%s) AND (status='READY' OR (status='FAILED' AND retry_count < :maxRetries))")
                .formatted(messagesTable);
    }

    @Override
    public String fetchClaimedByIdsSqlTemplate() {
        return "SELECT id, `key` AS message_key, content, retry_count, container_id "
                + "FROM %s WHERE id IN (%%s) ORDER BY `key`, id".formatted(messagesTable);
    }

    @Override
    public String markProcessedSql() {
        return "UPDATE %s SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() WHERE id=:id"
                .formatted(messagesTable);
    }

    @Override
    public String markReadyForRetrySql() {
        return "UPDATE %s SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id"
                .formatted(messagesTable);
    }

    @Override
    public String markStoppedSql() {
        return "UPDATE %s SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id"
                .formatted(messagesTable);
    }

    @Override
    public String heartbeatUpsertSql() {
        return ("INSERT INTO %s (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                        + "ON DUPLICATE KEY UPDATE last_heartbeat = VALUES(last_heartbeat)")
                .formatted(heartbeatsTable);
    }

    @Override
    public String reclaimStaleProcessingSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL "
                        + "WHERE status='PROCESSING' AND last_updated < TIMESTAMPADD(SECOND, -:t, NOW())")
                .formatted(messagesTable);
    }

    @Override
    public String reclaimMissingHeartbeatsSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL WHERE status='PROCESSING' "
                        + "AND (container_id IS NULL OR container_id NOT IN ("
                        + "SELECT container_id FROM %s "
                        + "WHERE last_heartbeat >= TIMESTAMPADD(SECOND, -:hb, NOW())))")
                .formatted(messagesTable, heartbeatsTable);
    }
}
