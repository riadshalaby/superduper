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
        return ("INSERT INTO %s (message_id, message_key, content, status, occurred_at, received_at, retry_count, "
                        + "last_updated, correlation_id, message_type) "
                        + "VALUES (:messageId, :messageKey, :content, 'READY', :occurredAt, NOW(), 0, NOW(), "
                        + ":correlationId, :messageType) "
                        + "ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)")
                .formatted(messagesTable);
    }

    @Override
    public String claimBatchSql() {
        return ("UPDATE %s m "
                        + "JOIN ("
                        + "SELECT id FROM ("
                        + "SELECT m1.id FROM %s m1 "
                        + "LEFT JOIN %s p ON p.message_key = m1.message_key AND p.status = 'PROCESSING' "
                        + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                        + "AND p.id IS NULL "
                        + "ORDER BY m1.id LIMIT :batch FOR UPDATE SKIP LOCKED"
                        + ") candidate_ids"
                        + ") c ON c.id = m.id "
                        + "SET m.status='PROCESSING', m.container_id=:cid, m.last_updated=NOW()")
                .formatted(messagesTable, messagesTable, messagesTable);
    }

    @Override
    public String fetchClaimedForWorkerSql() {
        return ("SELECT id, message_id, message_key, content, retry_count, container_id, correlation_id, message_type "
                        + "FROM %s WHERE status='PROCESSING' AND container_id=:cid ORDER BY message_key, id")
                .formatted(messagesTable);
    }

    @Override
    public String findByStatusSql() {
        return ("SELECT id, message_id, message_key, content, retry_count, container_id, correlation_id, message_type "
                        + "FROM %s WHERE status = :status ORDER BY id LIMIT :limit")
                .formatted(messagesTable);
    }

    @Override
    public String releaseMessagesSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() "
                        + "WHERE id IN (:ids) AND container_id=:cid")
                .formatted(messagesTable);
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
    public String redriveByIdSql() {
        return ("UPDATE %s SET status='READY', retry_count=0, container_id=NULL, last_updated=NOW() "
                        + "WHERE id=:id AND status IN ('FAILED', 'STOPPED')")
                .formatted(messagesTable);
    }

    @Override
    public String redriveByStatusSql() {
        return ("UPDATE %s m JOIN (SELECT id FROM (SELECT id FROM %s WHERE status = :status ORDER BY id LIMIT :limit) "
                        + "redrive_ids) r ON r.id = m.id "
                        + "SET m.status='READY', m.retry_count=0, m.container_id=NULL, m.last_updated=NOW()")
                .formatted(messagesTable, messagesTable);
    }

    @Override
    public String heartbeatUpsertSql() {
        return ("INSERT INTO %s (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                        + "ON DUPLICATE KEY UPDATE last_heartbeat = VALUES(last_heartbeat)")
                .formatted(heartbeatsTable);
    }

    @Override
    public String countByStatusSql() {
        return ("SELECT status, COUNT(*) AS cnt FROM %s GROUP BY status").formatted(messagesTable);
    }

    @Override
    public String reclaimStaleProcessingSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() "
                        + "WHERE status='PROCESSING' AND last_updated < TIMESTAMPADD(SECOND, -:t, NOW())")
                .formatted(messagesTable);
    }

    @Override
    public String reclaimMissingHeartbeatsSql() {
        return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() WHERE status='PROCESSING' "
                        + "AND (container_id IS NULL OR container_id NOT IN ("
                        + "SELECT container_id FROM %s "
                        + "WHERE last_heartbeat >= TIMESTAMPADD(SECOND, -:hb, NOW())))")
                .formatted(messagesTable, heartbeatsTable);
    }

    @Override
    public String deleteProcessedOlderThanSql() {
        return ("DELETE FROM %s WHERE status='PROCESSED' "
                        + "AND last_updated < TIMESTAMPADD(DAY, -:retentionDays, NOW())")
                .formatted(messagesTable);
    }

    @Override
    public String deleteStoppedOlderThanSql() {
        return ("DELETE FROM %s WHERE status='STOPPED' "
                        + "AND last_updated < TIMESTAMPADD(DAY, -:retentionDays, NOW())")
                .formatted(messagesTable);
    }

    @Override
    public String deleteStaleHeartbeatsSql() {
        return ("DELETE FROM %s WHERE last_heartbeat < TIMESTAMPADD(DAY, -:retentionDays, NOW())")
                .formatted(heartbeatsTable);
    }
}
