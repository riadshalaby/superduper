package net.rsworld.superduper.repository.jdbc;

public interface JdbcSqlDialect {
    String upsertReadyMessageSql();

    String claimBatchSql();

    String fetchClaimedForWorkerSql();

    String findByStatusSql();

    String releaseMessagesSql();

    String markProcessedSql();

    String markFailedSql();

    String markStoppedSql();

    String redriveByIdSql();

    String redriveByStatusSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();

    String heartbeatUpsertSql();

    String countByStatusSql();
}
