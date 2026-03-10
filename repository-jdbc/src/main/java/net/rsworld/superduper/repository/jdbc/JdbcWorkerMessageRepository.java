package net.rsworld.superduper.repository.jdbc;

import java.util.List;
import java.util.Map;
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
        return claimBatch(workerId, batchSize, maxRetries, "default");
    }

    @Override
    public long claimBatch(String workerId, int batchSize, int maxRetries, String topic) {
        return jdbc.update(
                dialect.claimBatchSql(),
                new MapSqlParameterSource()
                        .addValue("cid", workerId)
                        .addValue("batch", batchSize)
                        .addValue("maxRetries", maxRetries)
                        .addValue("topic", topic));
    }

    @Override
    public List<ClaimedMessage> fetchClaimedForWorker(String workerId) {
        return fetchClaimedForWorker(workerId, "default");
    }

    @Override
    public List<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic) {
        return jdbc.query(
                dialect.fetchClaimedForWorkerSql(),
                new MapSqlParameterSource().addValue("cid", workerId).addValue("topic", topic),
                claimedMessageRowMapper());
    }

    @Override
    public List<ClaimedMessage> findByStatus(String status, int limit) {
        return findByStatus(status, limit, "default");
    }

    @Override
    public List<ClaimedMessage> findByStatus(String status, int limit, String topic) {
        return jdbc.query(
                dialect.findByStatusSql(),
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("limit", limit)
                        .addValue("topic", topic),
                claimedMessageRowMapper());
    }

    @Override
    public int redriveById(long id) {
        return jdbc.update(dialect.redriveByIdSql(), new MapSqlParameterSource().addValue("id", id));
    }

    @Override
    public int redriveByStatus(String status, int limit) {
        return redriveByStatus(status, limit, "default");
    }

    @Override
    public int redriveByStatus(String status, int limit, String topic) {
        return jdbc.update(
                dialect.redriveByStatusSql(),
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("limit", limit)
                        .addValue("topic", topic));
    }

    @Override
    public Map<String, Long> countByStatus() {
        return countByStatus("default");
    }

    @Override
    public Map<String, Long> countByStatus(String topic) {
        return jdbc.query(dialect.countByStatusSql(), new MapSqlParameterSource().addValue("topic", topic), rs -> {
            Map<String, Long> counts = new java.util.LinkedHashMap<>();
            while (rs.next()) {
                counts.put(rs.getString("status"), rs.getLong("cnt"));
            }
            return counts;
        });
    }

    private static org.springframework.jdbc.core.RowMapper<ClaimedMessage> claimedMessageRowMapper() {
        return (rs, rn) -> new ClaimedMessage(
                rs.getLong("id"),
                rs.getString("topic"),
                rs.getString("message_id"),
                rs.getString("message_key"),
                rs.getString("content"),
                rs.getInt("retry_count"),
                rs.getString("container_id"),
                rs.getString("correlation_id"),
                rs.getString("message_type"));
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
