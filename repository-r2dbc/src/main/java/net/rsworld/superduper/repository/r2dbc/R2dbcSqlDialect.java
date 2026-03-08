package net.rsworld.superduper.repository.r2dbc;

public interface R2dbcSqlDialect {
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

    String heartbeatUpsertSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();

    String countByStatusSql();
}
