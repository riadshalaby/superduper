package net.rsworld.superduper.repository.r2dbc;

public final class PostgresWorkerR2dbcSqlDialect implements WorkerR2dbcSqlDialect {

    @Override
    public String heartbeatUpsertSql() {
        return "INSERT INTO container_heartbeats (container_id, last_heartbeat) VALUES (:cid, NOW()) "
                + "ON CONFLICT (container_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat";
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
}
