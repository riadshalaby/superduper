package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
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
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.PostgresR2dbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.SqlDialect;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@Testcontainers
class WorkerReactiveIntegrationTest {
    private static final Logger log = LogManager.getLogger(WorkerReactiveIntegrationTest.class);

    static PostgreSQLContainer pg;
    static DatabaseClient db;
    static ReactiveWorkerMessageRepository messageRepository;
    static ReactiveWorkerMaintenanceRepository maintenanceRepository;

    @BeforeAll
    static void setup() {
        pg = new PostgreSQLContainer("postgres:16-alpine");
        pg.start();
        LiquibaseTestSupport.migrate(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        messageRepository = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.POSTGRES);
        maintenanceRepository = new R2dbcWorkerMaintenanceRepository(db, new PostgresR2dbcSqlDialect());
    }

    @AfterAll
    static void tearDown() {
        if (pg != null) pg.stop();
    }

    @Test
    void claim_process_then_claim_next() {
        resetData();
        seedRows("ru1", "k1", "v1", "READY", "ru2", "k1", "v2", "READY", "ru3", "k2", "v3", "READY");

        ReactiveMessageHandler handler = r -> Mono.just(ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                messageRepository,
                org.mockito.Mockito.mock(LockingTaskExecutor.class),
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);

        Long first = messageRepository.claimBatch("w1", 10, 5).block();
        StepVerifier.create(Mono.just(first == null ? 0L : first))
                .expectNext(2L)
                .verifyComplete();

        var list = messageRepository.fetchClaimedForWorker("w1").collectList().block();
        for (var row : list) {
            processRow(svc, row);
        }

        Long second = messageRepository.claimBatch("w1", 10, 5).block();
        StepVerifier.create(Mono.just(second == null ? 0L : second))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void reactive_heartbeat_and_orphan_reclaimer() {
        resetData();
        seedRows("ru3", "k2", "v3", "READY");

        db.sql("UPDATE messages SET status='PROCESSING', last_updated = NOW() - INTERVAL '10 minutes' WHERE uuid='ru3'")
                .fetch()
                .rowsUpdated()
                .block();

        ReactiveHeartbeatService hb = new ReactiveHeartbeatService(
                maintenanceRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10_000);
        hb.heartbeat();

        ReactiveOrphanReclaimer rec = new ReactiveOrphanReclaimer(
                maintenanceRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                120_000,
                10_000);
        rec.reclaim();

        Mono<String> status = db.sql("SELECT status FROM messages WHERE uuid='ru3'")
                .map((r, m) -> r.get("status", String.class))
                .one();
        StepVerifier.create(status).expectNext("READY").verifyComplete();
    }

    @Test
    void concurrent_claims_do_not_overlap() {
        resetData();
        seedRows("cu1", "ck1", "a", "READY", "cu2", "ck1", "b", "READY", "cu3", "ck2", "c", "READY");

        Mono<Long> c1 = messageRepository.claimBatch("w1", 10, 5).subscribeOn(Schedulers.parallel());
        Mono<Long> c2 = messageRepository.claimBatch("w2", 10, 5).subscribeOn(Schedulers.parallel());

        var result = Mono.zip(c1, c2).block();
        assertThat(result).isNotNull();

        Long first = result.getT1();
        Long second = result.getT2();
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first + second).isEqualTo(2L);

        Integer processing = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSING'")
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
        assertThat(processing).isEqualTo(2);
    }

    @Test
    void parallel_workers_process_all_messages() throws Exception {
        final int messageCount = 1500;
        final int workerCount = 5;
        final int batchSize = 100;
        final int timeoutSeconds = 60;

        resetData();
        seedRowsWithGenerateSeries(messageCount);

        ReactiveMessageHandler handler = row -> Mono.just(ProcessingResult.SUCCESS);
        LockingTaskExecutor passthroughLockExec = passthroughLockExecutor();
        RecordingObserver observer = new RecordingObserver();

        List<SuperDuperWorkerReactiveService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "reactive-it-worker-" + i;
            workers.add(new SuperDuperWorkerReactiveService(
                    messageRepository,
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
            for (SuperDuperWorkerReactiveService worker : workers) {
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
        Integer notProcessed = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status <> 'PROCESSED'")
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
        assertThat(notProcessed).isEqualTo(0);
        assertThat(observer.totalClaimed()).isGreaterThanOrEqualTo(messageCount);
        assertThat(observer.processedEvents()).isGreaterThanOrEqualTo(messageCount);
        assertThat(observer.failedEvents()).isEqualTo(0);
        assertThat(observer.claimObservations()).isNotEmpty();
        assertThat(observer.uniqueProcessedMessageCount()).isEqualTo(messageCount);
        assertThat(observer.claimedByWorker()).isNotEmpty();
        assertThat(observer.processedByWorker()).isNotEmpty();
        assertThat(observer.claimObservations())
                .allMatch(o -> "reactive".equals(o.mode())
                        && o.workerId() != null
                        && !o.workerId().isBlank());
        assertThat(observer.processedObservations())
                .allMatch(o -> "reactive".equals(o.mode()) && o.messageId() != null && o.messageId() > 0);

        log.info("REACTIVE WORKER CLAIMED BY workerId: {}", observer.claimedByWorker());
        log.info("REACTIVE WORKER PROCESSED BY workerId: {}", observer.processedByWorker());
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
        ReactiveMessageHandler handler = row -> {
            processedOrder.add(row.id());
            return Mono.just(ProcessingResult.SUCCESS);
        };

        List<SuperDuperWorkerReactiveService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "reactive-it-worker-strict-" + i;
            workers.add(new SuperDuperWorkerReactiveService(
                    messageRepository,
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
            for (SuperDuperWorkerReactiveService worker : workers) {
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
        ReactiveMessageHandler handler = row -> {
            processedOrderByKey
                    .computeIfAbsent(row.key(), ignored -> java.util.Collections.synchronizedList(new ArrayList<>()))
                    .add(row.id());
            return Mono.just(ProcessingResult.SUCCESS);
        };

        List<SuperDuperWorkerReactiveService> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "reactive-it-worker-mixed-" + i;
            workers.add(new SuperDuperWorkerReactiveService(
                    messageRepository,
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
            for (SuperDuperWorkerReactiveService worker : workers) {
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
        int processedTotal =
                processedOrderByKey.values().stream().mapToInt(List::size).sum();
        assertThat(processedTotal).isEqualTo(messageCount);
        expectedOrderByKey.forEach((key, expectedIds) -> assertThat(processedOrderByKey.get(key))
                .as("processed order for key %s", key)
                .containsExactlyElementsOf(expectedIds));
    }

    private static void processRow(
            SuperDuperWorkerReactiveService svc, net.rsworld.superduper.repository.api.ClaimedMessage row) {
        try {
            var processOne = SuperDuperWorkerReactiveService.class.getDeclaredMethod(
                    "processOne", net.rsworld.superduper.repository.api.ClaimedMessage.class);
            processOne.setAccessible(true);
            ((Mono<Void>) processOne.invoke(svc, row)).block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void resetData() {
        Flux.concat(
                        db.sql("TRUNCATE TABLE messages RESTART IDENTITY")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("TRUNCATE TABLE container_heartbeats").fetch().rowsUpdated())
                .then()
                .block();
    }

    private static int processedCount() {
        Integer processed = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSED'")
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
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
        db.sql("INSERT INTO messages(uuid,key,content,status) "
                        + "SELECT LPAD(g::text, 36, '0'), 'parallel-key-' || g, 'payload-' || g, 'READY' "
                        + "FROM generate_series(1, "
                        + count
                        + ") g")
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static void seedRowsSingleKeyWithGenerateSeries(int count, String key) {
        db.sql("INSERT INTO messages(uuid,key,content,status) "
                        + "SELECT LPAD(g::text, 36, '0'), '"
                        + key
                        + "', 'payload-' || g, 'READY' "
                        + "FROM generate_series(1, "
                        + count
                        + ") g")
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static void seedRowsMixedKeyPattern(int repeatCount) {
        db.sql("INSERT INTO messages(uuid,key,content,status) "
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
                        + "ORDER BY (((g - 1) * 6) + p.seq)")
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static Map<String, List<Long>> expectedMessageOrderByKey() {
        List<Map.Entry<String, Long>> rows = db.sql("SELECT key, id FROM messages ORDER BY id")
                .map((r, m) -> Map.entry(r.get("key", String.class), r.get("id", Long.class)))
                .all()
                .collectList()
                .block();
        Map<String, List<Long>> result = new TreeMap<>();
        if (rows == null) {
            return result;
        }
        for (Map.Entry<String, Long> row : rows) {
            if (row.getKey() != null && row.getValue() != null) {
                result.computeIfAbsent(row.getKey(), ignored -> new ArrayList<>())
                        .add(row.getValue());
            }
        }
        return result;
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
        Flux.range(0, values.length / 4)
                .concatMap(i -> {
                    int offset = i * 4;
                    return db.sql("INSERT INTO messages(uuid,key,content,status) VALUES (:uuid,:key,:content,:status)")
                            .bind("uuid", values[offset])
                            .bind("key", values[offset + 1])
                            .bind("content", values[offset + 2])
                            .bind("status", values[offset + 3])
                            .fetch()
                            .rowsUpdated();
                })
                .then()
                .block();
    }
}
