package net.rsworld.superduper.repository.jdbc;

public interface JdbcSqlDialect {
    String upsertReadyMessageSql();

    String claimBatchSql();

    String fetchClaimedForWorkerSql();

    String releaseMessagesSql();

    String markProcessedSql();

    String markFailedSql();

    String markStoppedSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();

    String heartbeatUpsertSql();
}
