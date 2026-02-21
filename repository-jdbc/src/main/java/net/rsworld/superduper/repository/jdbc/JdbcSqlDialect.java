package net.rsworld.superduper.repository.jdbc;

public interface JdbcSqlDialect {
    String upsertReadyMessageSql();

    String claimBatchSql();

    String claimBatchUpdateSql();

    String fetchClaimedSql();

    String markProcessedSql();

    String markReadyForRetrySql();

    String markStoppedSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();

    String heartbeatUpsertSql();
}
