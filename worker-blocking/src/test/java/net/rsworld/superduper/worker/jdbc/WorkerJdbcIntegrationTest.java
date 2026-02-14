package net.rsworld.superduper.worker.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.PostgresWorkerJdbcSqlDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class WorkerJdbcIntegrationTest {

    static PostgreSQLContainer pg;
    static NamedParameterJdbcTemplate jdbc;
    static PlatformTransactionManager txm;
    static LockingTaskExecutor lockExec;
    static WorkerMessageRepository messageRepository;
    static WorkerMaintenanceRepository maintenanceRepository;

    @BeforeAll
    static void setup() {
        pg = new PostgreSQLContainer("postgres:16-alpine");
        pg.start();

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(pg.getJdbcUrl());
        ds.setUsername(pg.getUsername());
        ds.setPassword(pg.getPassword());

        jdbc = new NamedParameterJdbcTemplate(ds);
        txm = new DataSourceTransactionManager(ds);
        lockExec = mock(LockingTaskExecutor.class);
        messageRepository = new JdbcWorkerMessageRepository(jdbc);
        maintenanceRepository = new JdbcWorkerMaintenanceRepository(jdbc, new PostgresWorkerJdbcSqlDialect());

        jdbc.getJdbcTemplate()
                .execute(
                        "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())");
        jdbc.getJdbcTemplate()
                .execute(
                        "CREATE TABLE container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())");
        jdbc.getJdbcTemplate().execute("CREATE INDEX idx_messages_key_id ON messages(key, id)");
    }

    @AfterAll
    static void tearDown() {
        if (pg != null) {
            pg.stop();
        }
    }

    @Test
    void claim_process_then_claim_next() {
        resetData();
        seedRows("ju1", "k1", "v1", "READY", "ju2", "k1", "v2", "READY", "ju3", "k2", "v3", "READY");

        MessageHandler handler = r -> ProcessingResult.SUCCESS;
        SuperDuperWorkerService svc = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);

        List<Long> first = svc.claimBatch();
        assertThat(first).hasSize(2);

        svc.process(first);

        List<Long> second = svc.claimBatch();
        assertThat(second).hasSize(1);
    }

    @Test
    void heartbeat_and_orphan_reclaimer() {
        resetData();
        seedRows("ju3", "k2", "v3", "READY");

        jdbc.update(
                "UPDATE messages SET status='PROCESSING', last_updated = NOW() - INTERVAL '10 minutes' WHERE uuid=:uuid",
                new MapSqlParameterSource("uuid", "ju3"));

        HeartbeatService hb = new HeartbeatService(
                maintenanceRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10_000);
        hb.heartbeat();

        OrphanReclaimer rec = new OrphanReclaimer(
                maintenanceRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                120_000,
                10_000);
        rec.reclaim();

        String status = jdbc.queryForObject(
                "SELECT status FROM messages WHERE uuid=:uuid", new MapSqlParameterSource("uuid", "ju3"), String.class);
        assertThat(status).isEqualTo("READY");
    }

    @Test
    void concurrent_claims_do_not_overlap() throws Exception {
        resetData();
        seedRows("cu1", "ck1", "a", "READY", "cu2", "ck1", "b", "READY", "cu3", "ck2", "c", "READY");

        MessageHandler handler = r -> ProcessingResult.SUCCESS;
        SuperDuperWorkerService svc1 = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);
        SuperDuperWorkerService svc2 = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<Long>> c1 = executor.submit(svc1::claimBatch);
            Future<List<Long>> c2 = executor.submit(svc2::claimBatch);

            List<Long> first = c1.get(10, TimeUnit.SECONDS);
            List<Long> second = c2.get(10, TimeUnit.SECONDS);

            HashSet<Long> overlap = new HashSet<>(first);
            overlap.retainAll(second);
            assertThat(overlap).isEmpty();

            HashSet<Long> allClaimed = new HashSet<>();
            allClaimed.addAll(first);
            allClaimed.addAll(second);
            assertThat(allClaimed).hasSize(2);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages RESTART IDENTITY");
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE container_heartbeats");
    }

    private static void seedRows(String... values) {
        if (values.length % 4 != 0) {
            throw new IllegalArgumentException("Expected tuples of (uuid, key, content, status)");
        }
        for (int i = 0; i < values.length; i += 4) {
            jdbc.update(
                    "INSERT INTO messages(uuid,key,content,status) VALUES (:uuid,:key,:content,:status)",
                    new MapSqlParameterSource()
                            .addValue("uuid", values[i])
                            .addValue("key", values[i + 1])
                            .addValue("content", values[i + 2])
                            .addValue("status", values[i + 3]));
        }
    }
}
