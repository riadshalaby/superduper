package net.rsworld.superduper.worker.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.PostgresJdbcSqlDialect;
import net.rsworld.superduper.repository.jdbc.SqlDialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
class WorkerBlockingIntegrationTest {
    private static final Logger log = LogManager.getLogger(WorkerBlockingIntegrationTest.class);

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
        messageRepository = new JdbcWorkerMessageRepository(jdbc, SqlDialect.POSTGRES);
        maintenanceRepository = new JdbcWorkerMaintenanceRepository(jdbc, new PostgresJdbcSqlDialect());

        jdbc.getJdbcTemplate()
                .execute(
                        "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())");
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

        long first = svc.claimBatch();
        assertThat(first).isEqualTo(2L);

        svc.process();

        long second = svc.claimBatch();
        assertThat(second).isEqualTo(1L);
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
            Future<Long> c1 = executor.submit(svc1::claimBatch);
            Future<Long> c2 = executor.submit(svc2::claimBatch);

            long first = c1.get(10, TimeUnit.SECONDS);
            long second = c2.get(10, TimeUnit.SECONDS);
            assertThat(first + second).isEqualTo(2L);

            Integer processing = jdbc.getJdbcTemplate()
                    .queryForObject("SELECT COUNT(*) FROM messages WHERE status='PROCESSING'", Integer.class);
            assertThat(processing).isEqualTo(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void parallel_workers_process_all_messages() throws Exception {
        final int messageCount = 1500;
        final int workerCount = 5;
        final int batchSize = 100;
        final int timeoutSeconds = 60;

        resetData();
        seedRowsWithGenerateSeries(messageCount);

        MessageHandler handler = r -> ProcessingResult.SUCCESS;
        LockingTaskExecutor passthroughLockExec = passthroughLockExecutor();
        RecordingObserver observer = new RecordingObserver();

        List<SuperDuperWorkerService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "blocking-it-worker-" + i;
            workers.add(new SuperDuperWorkerService(
                    messageRepository,
                    txm,
                    passthroughLockExec,
                    handler,
                    observer,
                    batchSize,
                    5,
                    "superduper-claim-batch",
                    15000,
                    2000,
                    workerId));
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (SuperDuperWorkerService worker : workers) {
                futures.add(executor.submit(() -> {
                    start.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
                    while (System.nanoTime() < deadline) {
                        worker.schedule();
                        if (processedCount() >= messageCount) {
                            return null;
                        }
                    }
                    throw new AssertionError("Timed out waiting for all messages to be processed");
                }));
            }
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(timeoutSeconds + 10L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(processedCount()).isEqualTo(messageCount);
        Integer notProcessed = jdbc.getJdbcTemplate()
                .queryForObject("SELECT COUNT(*) FROM messages WHERE status <> 'PROCESSED'", Integer.class);
        assertThat(notProcessed).isEqualTo(0);
        assertThat(observer.totalClaimed()).isGreaterThanOrEqualTo(messageCount);
        assertThat(observer.processedEvents()).isGreaterThanOrEqualTo(messageCount);
        assertThat(observer.failedEvents()).isEqualTo(0);
        assertThat(observer.claimObservations()).isNotEmpty();
        assertThat(observer.uniqueProcessedMessageCount()).isEqualTo(messageCount);
        assertThat(observer.claimedByWorker()).isNotEmpty();
        assertThat(observer.processedByWorker()).isNotEmpty();
        assertThat(observer.claimObservations())
                .allMatch(o -> "blocking".equals(o.mode())
                        && o.workerId() != null
                        && !o.workerId().isBlank());
        assertThat(observer.processedObservations())
                .allMatch(o -> "blocking".equals(o.mode()) && o.messageId() != null && o.messageId() > 0);

        log.info("BLOCKING WORKER CLAIMED BY workerId: {}", observer.claimedByWorker());
        log.info("BLOCKING WORKER PROCESSED BY workerId: {}", observer.processedByWorker());
    }

    @Test
    void parallel_workers_preserve_strict_per_key_ordering() throws Exception {
        final int messageCount = 300;
        final int workerCount = 5;
        final int batchSize = 100;
        final int timeoutSeconds = 60;

        resetData();
        seedRowsSingleKeyWithGenerateSeries(messageCount, "strict-key");

        LockingTaskExecutor passthroughLockExec = passthroughLockExecutor();
        List<Long> processedOrder = java.util.Collections.synchronizedList(new ArrayList<>());
        MessageHandler handler = row -> {
            processedOrder.add(row.id());
            return ProcessingResult.SUCCESS;
        };

        List<SuperDuperWorkerService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "blocking-it-worker-strict-" + i;
            workers.add(new SuperDuperWorkerService(
                    messageRepository,
                    txm,
                    passthroughLockExec,
                    handler,
                    net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                    batchSize,
                    5,
                    "superduper-claim-batch",
                    15000,
                    2000,
                    workerId));
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (SuperDuperWorkerService worker : workers) {
                futures.add(executor.submit(() -> {
                    start.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
                    while (System.nanoTime() < deadline) {
                        worker.schedule();
                        if (processedCount() >= messageCount) {
                            return null;
                        }
                    }
                    throw new AssertionError("Timed out waiting for strict-key sequence to be processed");
                }));
            }
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(timeoutSeconds + 10L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(processedCount()).isEqualTo(messageCount);
        assertThat(processedOrder).hasSize(messageCount);
        assertThat(processedOrder)
                .containsExactlyElementsOf(
                        LongStream.rangeClosed(1, messageCount).boxed().toList());
    }

    @Test
    void parallel_workers_preserve_per_key_ordering_with_mixed_keys() throws Exception {
        final int patternRepeats = 100;
        final int messageCount = patternRepeats * 6;
        final int workerCount = 5;
        final int batchSize = 100;
        final int timeoutSeconds = 60;

        resetData();
        seedRowsMixedKeyPattern(patternRepeats);
        Map<String, List<Long>> expectedOrderByKey = expectedMessageOrderByKey();

        LockingTaskExecutor passthroughLockExec = passthroughLockExecutor();
        Map<String, List<Long>> processedOrderByKey = new ConcurrentHashMap<>();
        MessageHandler handler = row -> {
            processedOrderByKey
                    .computeIfAbsent(row.key(), ignored -> java.util.Collections.synchronizedList(new ArrayList<>()))
                    .add(row.id());
            return ProcessingResult.SUCCESS;
        };

        List<SuperDuperWorkerService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "blocking-it-worker-mixed-" + i;
            workers.add(new SuperDuperWorkerService(
                    messageRepository,
                    txm,
                    passthroughLockExec,
                    handler,
                    net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                    batchSize,
                    5,
                    "superduper-claim-batch",
                    15000,
                    2000,
                    workerId));
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (SuperDuperWorkerService worker : workers) {
                futures.add(executor.submit(() -> {
                    start.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
                    while (System.nanoTime() < deadline) {
                        worker.schedule();
                        if (processedCount() >= messageCount) {
                            return null;
                        }
                    }
                    throw new AssertionError("Timed out waiting for mixed-key sequence to be processed");
                }));
            }
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(timeoutSeconds + 10L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(processedCount()).isEqualTo(messageCount);
        assertThat(processedOrderByKey.keySet()).containsExactlyInAnyOrderElementsOf(expectedOrderByKey.keySet());
        int processedTotal = processedOrderByKey.values().stream().mapToInt(List::size).sum();
        assertThat(processedTotal).isEqualTo(messageCount);
        expectedOrderByKey.forEach((key, expectedIds) -> assertThat(processedOrderByKey.get(key))
                .as("processed order for key %s", key)
                .containsExactlyElementsOf(expectedIds));
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages RESTART IDENTITY");
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE container_heartbeats");
    }

    private static int processedCount() {
        Integer processed = jdbc.getJdbcTemplate()
                .queryForObject("SELECT COUNT(*) FROM messages WHERE status='PROCESSED'", Integer.class);
        return processed == null ? 0 : processed;
    }

    private static LockingTaskExecutor passthroughLockExecutor() {
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        doAnswer(invocation -> {
                    Runnable runnable = invocation.getArgument(0);
                    runnable.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());
        return lockExec;
    }

    private static void seedRowsWithGenerateSeries(int count) {
        jdbc.getJdbcTemplate()
                .execute("INSERT INTO messages(uuid,key,content,status) "
                        + "SELECT LPAD(g::text, 36, '0'), 'parallel-key-' || g, 'payload-' || g, 'READY' "
                        + "FROM generate_series(1, "
                        + count
                        + ") g");
    }

    private static void seedRowsSingleKeyWithGenerateSeries(int count, String key) {
        jdbc.getJdbcTemplate()
                .execute("INSERT INTO messages(uuid,key,content,status) "
                        + "SELECT LPAD(g::text, 36, '0'), '"
                        + key
                        + "', 'payload-' || g, 'READY' "
                        + "FROM generate_series(1, "
                        + count
                        + ") g");
    }

    private static void seedRowsMixedKeyPattern(int repeatCount) {
        jdbc.getJdbcTemplate()
                .execute("INSERT INTO messages(uuid,key,content,status) "
                        + "SELECT LPAD((((g - 1) * 6) + p.seq)::text, 36, '0'), p.msg_key, p.msg_content || '-' || g, 'READY' "
                        + "FROM generate_series(1, "
                        + repeatCount
                        + ") g "
                        + "CROSS JOIN (VALUES "
                        + "(1, 'key-1', 'k1-m1'), "
                        + "(2, 'key-2', 'k2-m1'), "
                        + "(3, 'key-2', 'k2-m2'), "
                        + "(4, 'key-1', 'k1-m2'), "
                        + "(5, 'key-3', 'k3-m1'), "
                        + "(6, 'key-1', 'k1-m3')) AS p(seq, msg_key, msg_content) "
                        + "ORDER BY (((g - 1) * 6) + p.seq)");
    }

    private static Map<String, List<Long>> expectedMessageOrderByKey() {
        return jdbc.getJdbcTemplate().query("SELECT key, id FROM messages ORDER BY id", rs -> {
            Map<String, List<Long>> result = new TreeMap<>();
            while (rs.next()) {
                result.computeIfAbsent(rs.getString("key"), ignored -> new ArrayList<>())
                        .add(rs.getLong("id"));
            }
            return result;
        });
    }

    private static final class RecordingObserver implements SuperduperObserver {
        private final AtomicInteger totalClaimed = new AtomicInteger();
        private final AtomicInteger processedEvents = new AtomicInteger();
        private final AtomicInteger failedEvents = new AtomicInteger();
        private final ConcurrentLinkedQueue<WorkerObservation> claimObservations = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<WorkerObservation> processedObservations = new ConcurrentLinkedQueue<>();
        private final ConcurrentHashMap<String, AtomicInteger> claimedByWorker = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> processedByWorker = new ConcurrentHashMap<>();

        @Override
        public void consumerReceived(ConsumerObservation observation) {}

        @Override
        public void consumerSucceeded(ConsumerObservation observation) {}

        @Override
        public void consumerFailed(ConsumerObservation observation, Throwable error) {}

        @Override
        public void workerClaimed(WorkerObservation observation, int claimedCount) {
            claimObservations.add(observation);
            totalClaimed.addAndGet(claimedCount);
            if (observation.workerId() != null) {
                claimedByWorker
                        .computeIfAbsent(observation.workerId(), k -> new AtomicInteger())
                        .addAndGet(claimedCount);
            }
        }

        @Override
        public void workerProcessed(WorkerObservation observation) {
            processedObservations.add(observation);
            processedEvents.incrementAndGet();
            if (observation.workerId() != null) {
                processedByWorker
                        .computeIfAbsent(observation.workerId(), k -> new AtomicInteger())
                        .incrementAndGet();
            }
        }

        @Override
        public void workerRetried(WorkerObservation observation) {}

        @Override
        public void workerStopped(WorkerObservation observation) {}

        @Override
        public void workerFailed(WorkerObservation observation, Throwable error) {
            failedEvents.incrementAndGet();
        }

        @Override
        public void maintenanceSucceeded(MaintenanceObservation observation) {}

        @Override
        public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {}

        int totalClaimed() {
            return totalClaimed.get();
        }

        int processedEvents() {
            return processedEvents.get();
        }

        int failedEvents() {
            return failedEvents.get();
        }

        List<WorkerObservation> claimObservations() {
            return List.copyOf(claimObservations);
        }

        List<WorkerObservation> processedObservations() {
            return List.copyOf(processedObservations);
        }

        int uniqueProcessedMessageCount() {
            return (int) processedObservations.stream()
                    .map(WorkerObservation::messageId)
                    .filter(id -> id != null && id > 0)
                    .distinct()
                    .count();
        }

        Map<String, Integer> claimedByWorker() {
            Map<String, Integer> snapshot = new TreeMap<>();
            claimedByWorker.forEach((key, value) -> snapshot.put(key, value.get()));
            return snapshot;
        }

        Map<String, Integer> processedByWorker() {
            Map<String, Integer> snapshot = new TreeMap<>();
            processedByWorker.forEach((key, value) -> snapshot.put(key, value.get()));
            return snapshot;
        }
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
