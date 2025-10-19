package net.rsworld.superduper.worker.jdbc;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class SuperDuperWorkerService {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final LockingTaskExecutor lockExec;
    private final MessageHandler handler;
    private final String workerId;
    private final int batch;
    private final int maxRetries;

    public SuperDuperWorkerService(
            NamedParameterJdbcTemplate jdbc,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            @Value("${superduper.worker.batch-size:100}") int batch,
            @Value("${superduper.worker.max-retries:5}") int maxRetries) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(txm);
        this.lockExec = lockExec;
        this.handler = handler;
        this.batch = batch;
        this.maxRetries = maxRetries;
        this.workerId = ManagementFactory.getRuntimeMXBean().getName();
    }

    @Scheduled(fixedDelayString = "${superduper.worker.claim-interval-ms:5000}", initialDelay = 3000)
    public void schedule() {
        final List<Integer> ids = new ArrayList<>();
        lockExec.executeWithLock(
                (Runnable) () -> ids.addAll(claimBatch()),
                new LockConfiguration(
                        Instant.now(), "superduper-claim-batch", Duration.ofSeconds(15), Duration.ofSeconds(2)));
        if (!ids.isEmpty()) process(ids);
    }

    List<Integer> claimBatch() {
        return tx.execute(st -> {
            var params = new MapSqlParameterSource()
                    .addValue("batch", batch)
                    .addValue("maxRetries", maxRetries)
                    .addValue("cid", workerId);
            String sql = "WITH candidate AS (" + " SELECT m1.id FROM messages m1 "
                    + " LEFT JOIN messages p ON p.key = m1.key AND p.status = 'PROCESSING' "
                    + " WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
                    + "   AND p.id IS NULL "
                    + "   AND NOT EXISTS (SELECT 1 FROM messages prev WHERE prev.key = m1.key AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "
                    + " ORDER BY m1.id LIMIT :batch) "
                    + "UPDATE messages m SET status='PROCESSING', container_id=:cid, last_updated=now() FROM candidate c WHERE m.id = c.id RETURNING m.id";
            return jdbc.query(sql, params, (rs, rn) -> rs.getInt(1));
        });
    }

    void process(List<Integer> ids) {
        String inClause = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
        var rows = jdbc.getJdbcTemplate()
                .query(
                        "SELECT id, key, content, status, retry_count, container_id FROM messages WHERE id IN ("
                                + inClause + ") ORDER BY key, id",
                        (rs, rn) -> new MessageRow(
                                rs.getInt("id"),
                                null,
                                rs.getString("key"),
                                rs.getString("content"),
                                rs.getString("status"),
                                rs.getInt("retry_count"),
                                rs.getString("container_id")));

        for (var row : rows) {
            ProcessingResult res = ProcessingResult.RETRY;
            try {
                res = handler.handle(row);
            } catch (Exception ignored) {
            }
            int retry = row.retryCount() == null ? 0 : row.retryCount();
            if (res == ProcessingResult.SUCCESS) {
                jdbc.update(
                        "UPDATE messages SET status='PROCESSED', last_updated=now() WHERE id=:id",
                        new MapSqlParameterSource().addValue("id", row.id()));
            } else {
                retry++;
                if (retry < maxRetries) {
                    jdbc.update(
                            "UPDATE messages SET status='READY', retry_count=:r, container_id=NULL, last_updated=now() WHERE id=:id",
                            new MapSqlParameterSource().addValue("r", retry).addValue("id", row.id()));
                } else {
                    jdbc.update(
                            "UPDATE messages SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=now() WHERE id=:id",
                            new MapSqlParameterSource().addValue("r", retry).addValue("id", row.id()));
                }
            }
        }
    }
}
