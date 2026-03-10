package net.rsworld.superduper.repository.jdbc;

import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class JdbcWorkerMaintenanceRepository implements WorkerMaintenanceRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcSqlDialect dialect;

    public JdbcWorkerMaintenanceRepository(NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    @Override
    public void heartbeat(String workerId) {
        jdbc.update(dialect.heartbeatUpsertSql(), new MapSqlParameterSource().addValue("cid", workerId));
    }

    @Override
    public int reclaimStaleProcessing(int orphanTimeoutSec) {
        return reclaimStaleProcessing(orphanTimeoutSec, "default");
    }

    @Override
    public int reclaimStaleProcessing(int orphanTimeoutSec, String topic) {
        return jdbc.update(
                dialect.reclaimStaleProcessingSql(),
                new MapSqlParameterSource().addValue("t", orphanTimeoutSec).addValue("topic", topic));
    }

    @Override
    public int reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return reclaimMissingHeartbeats(heartbeatWindowSec, "default");
    }

    @Override
    public int reclaimMissingHeartbeats(int heartbeatWindowSec, String topic) {
        return jdbc.update(
                dialect.reclaimMissingHeartbeatsSql(),
                new MapSqlParameterSource().addValue("hb", heartbeatWindowSec).addValue("topic", topic));
    }

    @Override
    public int deleteProcessedOlderThan(int retentionDays) {
        return deleteProcessedOlderThan(retentionDays, "default");
    }

    @Override
    public int deleteProcessedOlderThan(int retentionDays, String topic) {
        return jdbc.update(
                dialect.deleteProcessedOlderThanSql(),
                new MapSqlParameterSource()
                        .addValue("retentionDays", retentionDays)
                        .addValue("topic", topic));
    }

    @Override
    public int deleteStoppedOlderThan(int retentionDays) {
        return deleteStoppedOlderThan(retentionDays, "default");
    }

    @Override
    public int deleteStoppedOlderThan(int retentionDays, String topic) {
        return jdbc.update(
                dialect.deleteStoppedOlderThanSql(),
                new MapSqlParameterSource()
                        .addValue("retentionDays", retentionDays)
                        .addValue("topic", topic));
    }

    @Override
    public int deleteStaleHeartbeats(int retentionDays) {
        return jdbc.update(
                dialect.deleteStaleHeartbeatsSql(),
                new MapSqlParameterSource().addValue("retentionDays", retentionDays));
    }
}
