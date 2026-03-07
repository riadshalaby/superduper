package net.rsworld.superduper.repository.jdbc;

import java.util.List;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class JdbcWorkerMessageRepository implements WorkerMessageRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final JdbcSqlDialect dialect;

    public JdbcWorkerMessageRepository(NamedParameterJdbcTemplate jdbc, SqlDialect dialect) {
        this(jdbc, JdbcSqlDialects.from(dialect));
    }

    public JdbcWorkerMessageRepository(NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        this.jdbc = jdbc;
        this.dialect = dialect;
    }

    @Override
    public long claimBatch(String workerId, int batchSize, int maxRetries) {
        return jdbc.update(
                dialect.claimBatchSql(),
                new MapSqlParameterSource()
                        .addValue("cid", workerId)
                        .addValue("batch", batchSize)
                        .addValue("maxRetries", maxRetries));
    }

    @Override
    public List<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return jdbc.query(
                dialect.fetchClaimedForWorkerSql(),
                new MapSqlParameterSource().addValue("cid", workerId),
                (rs, rn) -> new ClaimedMessage(
                        rs.getLong("id"),
                        rs.getString("message_id"),
                        rs.getString("message_key"),
                        rs.getString("content"),
                        rs.getInt("retry_count"),
                        rs.getString("container_id"),
                        rs.getString("correlation_id"),
                        rs.getString("message_type")));
    }

    @Override
    public int releaseMessages(List<Long> ids, String containerId) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        return jdbc.update(
                dialect.releaseMessagesSql(),
                new MapSqlParameterSource().addValue("ids", ids).addValue("cid", containerId));
    }

    @Override
    public boolean markProcessed(long id, String containerId) {
        return jdbc.update(
                        dialect.markProcessedSql(),
                        new MapSqlParameterSource().addValue("id", id).addValue("cid", containerId))
                > 0;
    }

    @Override
    public boolean markFailed(long id, int retryCount, String containerId) {
        return jdbc.update(
                        dialect.markFailedSql(),
                        new MapSqlParameterSource()
                                .addValue("r", retryCount)
                                .addValue("id", id)
                                .addValue("cid", containerId))
                > 0;
    }

    @Override
    public boolean markStopped(long id, int retryCount, String containerId) {
        return jdbc.update(
                        dialect.markStoppedSql(),
                        new MapSqlParameterSource()
                                .addValue("r", retryCount)
                                .addValue("id", id)
                                .addValue("cid", containerId))
                > 0;
    }
}
