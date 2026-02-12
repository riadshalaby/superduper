package net.rsworld.superduper.repository.r2dbc;

public interface R2dbcSqlDialect {
    String upsertReadyMessageSql();

    String claimBatchSql();

    String fetchClaimedForWorkerSql();

    String markProcessedSql();

    String markReadyForRetrySql();

    String markStoppedSql();

    String heartbeatUpsertSql();

    String reclaimStaleProcessingSql();

    String reclaimMissingHeartbeatsSql();
}
