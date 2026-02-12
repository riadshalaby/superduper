package net.rsworld.superduper.repository.jdbc;

public final class MariaDbWorkerJdbcSqlDialect implements WorkerJdbcSqlDialect {

    @Override
    public String reclaimStaleProcessingSql() {
        return "UPDATE messages SET status='READY', container_id=NULL "
                + "WHERE status='PROCESSING' AND last_updated < TIMESTAMPADD(SECOND, -:t, NOW())";
    }

    @Override
    public String reclaimMissingHeartbeatsSql() {
        return "UPDATE messages SET status='READY', container_id=NULL WHERE status='PROCESSING' "
                + "AND (container_id IS NULL OR container_id NOT IN ("
                + "SELECT container_id FROM container_heartbeats "
                + "WHERE last_heartbeat >= TIMESTAMPADD(SECOND, -:hb, NOW())))";
    }

    @Override
    public String heartbeatUpsertSql() {
        return "INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                + "ON DUPLICATE KEY UPDATE last_heartbeat = VALUES(last_heartbeat)";
    }
}
