package net.rsworld.superduper.repository.jdbc;

import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class JdbcWorkerMaintenanceRepository implements WorkerMaintenanceRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final WorkerJdbcSqlDialect dialect;

    public JdbcWorkerMaintenanceRepository(NamedParameterJdbcTemplate jdbc, WorkerJdbcSqlDialect dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    @Override
    public void heartbeat(String workerId) {
        jdbc.update(dialect.heartbeatUpsertSql(), new MapSqlParameterSource().addValue("cid", workerId));
    }

    @Override
    public void reclaimStaleProcessing(int orphanTimeoutSec) {
        jdbc.update(dialect.reclaimStaleProcessingSql(), new MapSqlParameterSource().addValue("t", orphanTimeoutSec));
    }

    @Override
    public void reclaimMissingHeartbeats(int heartbeatWindowSec) {
        jdbc.update(
                dialect.reclaimMissingHeartbeatsSql(), new MapSqlParameterSource().addValue("hb", heartbeatWindowSec));
    }
}
