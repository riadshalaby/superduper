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
        return jdbc.update(
                dialect.reclaimStaleProcessingSql(), new MapSqlParameterSource().addValue("t", orphanTimeoutSec));
    }

    @Override
    public int reclaimMissingHeartbeats(int heartbeatWindowSec) {
        return jdbc.update(
                dialect.reclaimMissingHeartbeatsSql(), new MapSqlParameterSource().addValue("hb", heartbeatWindowSec));
    }

    @Override
    public int deleteProcessedOlderThan(int retentionDays) {
        return jdbc.update(
                dialect.deleteProcessedOlderThanSql(),
                new MapSqlParameterSource().addValue("retentionDays", retentionDays));
    }

    @Override
    public int deleteStoppedOlderThan(int retentionDays) {
        return jdbc.update(
                dialect.deleteStoppedOlderThanSql(),
                new MapSqlParameterSource().addValue("retentionDays", retentionDays));
    }

    @Override
    public int deleteStaleHeartbeats(int retentionDays) {
        return jdbc.update(
                dialect.deleteStaleHeartbeatsSql(),
                new MapSqlParameterSource().addValue("retentionDays", retentionDays));
    }
}
