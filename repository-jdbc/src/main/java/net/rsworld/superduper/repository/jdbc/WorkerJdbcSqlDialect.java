package net.rsworld.superduper.repository.jdbc;

public interface WorkerJdbcSqlDialect {
    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();

    String heartbeatUpsertSql();
}
