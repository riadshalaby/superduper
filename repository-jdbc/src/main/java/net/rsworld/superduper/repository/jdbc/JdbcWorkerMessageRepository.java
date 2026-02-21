package net.rsworld.superduper.repository.jdbc;

import java.util.List;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class JdbcWorkerMessageRepository implements WorkerMessageRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public JdbcWorkerMessageRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<Long> claimBatch(String workerId, int batchSize, int maxRetries) {
        var params = new MapSqlParameterSource().addValue("batch", batchSize).addValue("maxRetries", maxRetries);

        List<Long> candidateIds = jdbc.query(
                "SELECT m1.id FROM messages m1 "
                        + "LEFT JOIN messages p ON p.key = m1.key AND p.status = 'PROCESSING' "
                        + "WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                        + "AND p.id IS NULL "
                        + "AND NOT EXISTS (SELECT 1 FROM messages prev WHERE prev.key = m1.key "
                        + "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                        + "ORDER BY m1.id LIMIT :batch FOR UPDATE OF m1 SKIP LOCKED",
                params,
                (rs, rn) -> rs.getLong(1));

        if (candidateIds.isEmpty()) {
            return candidateIds;
        }

        jdbc.update(
                "UPDATE messages SET status='PROCESSING', container_id=:cid, last_updated=NOW() "
                        + "WHERE id IN (:ids) "
                        + "AND (status='READY' OR (status='FAILED' AND retry_count < :maxRetries))",
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
                "SELECT id, key, content, retry_count, container_id FROM messages WHERE id IN (:ids) ORDER BY key, id",
                new MapSqlParameterSource().addValue("ids", ids),
                (rs, rn) -> new ClaimedMessage(
                        rs.getLong("id"),
                        rs.getString("key"),
                        rs.getString("content"),
                        rs.getInt("retry_count"),
                        rs.getString("container_id")));
    }

    @Override
    public void markProcessed(long id) {
        jdbc.update(
                "UPDATE messages SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() WHERE id=:id",
                new MapSqlParameterSource().addValue("id", id));
    }

    @Override
    public void markReadyForRetry(long id, int retryCount) {
        jdbc.update(
                "UPDATE messages SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id",
                new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
    }

    @Override
    public void markStopped(long id, int retryCount) {
        jdbc.update(
                "UPDATE messages SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id",
                new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
    }
}
