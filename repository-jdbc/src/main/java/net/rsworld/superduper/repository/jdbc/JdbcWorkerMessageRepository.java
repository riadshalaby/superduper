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
    public List<Long> claimBatch(String workerId, int batchSize, int maxRetries) {
        var params = new MapSqlParameterSource().addValue("batch", batchSize).addValue("maxRetries", maxRetries);

        List<Long> candidateIds = jdbc.query(dialect.claimBatchSql(), params, (rs, rn) -> rs.getLong(1));

        if (candidateIds.isEmpty()) {
            return candidateIds;
        }

        jdbc.update(
                dialect.claimBatchUpdateSql(),
                new MapSqlParameterSource()
                        .addValue("cid", workerId)
                        .addValue("ids", candidateIds)
                        .addValue("maxRetries", maxRetries));

        return candidateIds;
    }

    @Override
    public List<ClaimedMessage> fetchClaimedByIds(List<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }

        return jdbc.query(
                dialect.fetchClaimedSql(),
                new MapSqlParameterSource().addValue("ids", ids),
                (rs, rn) -> new ClaimedMessage(
                        rs.getLong("id"),
                        rs.getString("message_key"),
                        rs.getString("content"),
                        rs.getInt("retry_count"),
                        rs.getString("container_id")));
    }

    @Override
    public void markProcessed(long id) {
        jdbc.update(dialect.markProcessedSql(), new MapSqlParameterSource().addValue("id", id));
    }

    @Override
    public void markReadyForRetry(long id, int retryCount) {
        jdbc.update(
                dialect.markReadyForRetrySql(),
                new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
    }

    @Override
    public void markStopped(long id, int retryCount) {
        jdbc.update(
                dialect.markStoppedSql(),
                new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
    }
}
