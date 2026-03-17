package net.rsworld.superduper.repository.r2dbc;

/** Supplies R2DBC-specific SQL statements for message ingest, worker processing, and maintenance. */
public interface R2dbcSqlDialect {
    /**
     * Returns the SQL used to insert or update a READY message.
     *
     * @return the READY message upsert statement
     */
    String upsertReadyMessageSql();

    /**
     * Returns the SQL used to claim a batch of messages for a worker.
     *
     * @return the batch claim statement
     */
    String claimBatchSql();

    /**
     * Returns the SQL used to fetch rows currently claimed by a worker.
     *
     * @return the claimed-row fetch statement
     */
    String fetchClaimedForWorkerSql();

    /**
     * Returns the SQL used to find rows by terminal status.
     *
     * @return the status lookup statement
     */
    String findByStatusSql();

    /**
     * Returns the SQL used to release claimed messages back to READY.
     *
     * @return the release statement
     */
    String releaseMessagesSql();

    /**
     * Returns the SQL used to mark a message as processed.
     *
     * @return the processed update statement
     */
    String markProcessedSql();

    /**
     * Returns the SQL used to mark a message as failed.
     *
     * @return the failed update statement
     */
    String markFailedSql();

    /**
     * Returns the SQL used to mark a message as stopped.
     *
     * @return the stopped update statement
     */
    String markStoppedSql();

    /**
     * Returns the SQL used to redrive a single message by identifier.
     *
     * @return the single-message redrive statement
     */
    String redriveByIdSql();

    /**
     * Returns the SQL used to redrive messages by status.
     *
     * @return the status-based redrive statement
     */
    String redriveByStatusSql();

    /**
     * Returns the SQL used to insert or refresh worker heartbeats.
     *
     * @return the heartbeat upsert statement
     */
    String heartbeatUpsertSql();

    /**
     * Returns the SQL used to reclaim stale processing rows.
     *
     * @return the stale-processing reclaim statement
     */
    String reclaimStaleProcessingSql();

    /**
     * Returns the SQL used to reclaim rows whose worker heartbeat is missing.
     *
     * @return the missing-heartbeat reclaim statement
     */
    String reclaimMissingHeartbeatsSql();

    /**
     * Returns the SQL used to delete old processed rows.
     *
     * @return the processed cleanup statement
     */
    String deleteProcessedOlderThanSql();

    /**
     * Returns the SQL used to delete old stopped rows.
     *
     * @return the stopped cleanup statement
     */
    String deleteStoppedOlderThanSql();

    /**
     * Returns the SQL used to delete stale heartbeat rows.
     *
     * @return the heartbeat cleanup statement
     */
    String deleteStaleHeartbeatsSql();

    /**
     * Returns the SQL used to count queued rows grouped by status.
     *
     * @return the backlog counting statement
     */
    String countByStatusSql();
}
