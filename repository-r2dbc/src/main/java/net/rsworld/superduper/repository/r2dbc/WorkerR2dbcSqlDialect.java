package net.rsworld.superduper.repository.r2dbc;

public interface WorkerR2dbcSqlDialect {
    String heartbeatUpsertSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();
}
