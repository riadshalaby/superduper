package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.util.HashSet;
import java.util.List;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.PostgresWorkerR2dbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMessageRepository;
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

    static PostgreSQLContainer pg;
    static DatabaseClient db;
    static ReactiveWorkerMessageRepository messageRepository;
    static ReactiveWorkerMaintenanceRepository maintenanceRepository;

    @BeforeAll
    static void setup() {
        pg = new PostgreSQLContainer("postgres:16-alpine");
        pg.start();

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        messageRepository =
                new R2dbcWorkerMessageRepository(db, TransactionalOperator.create(new R2dbcTransactionManager(cf)));
        maintenanceRepository = new R2dbcWorkerMaintenanceRepository(db, new PostgresWorkerR2dbcSqlDialect());

        Flux.concat(
                        db.sql(
                                        "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())")
                                .fetch()
                                .rowsUpdated(),
                        db.sql(
                                        "CREATE TABLE container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("CREATE INDEX idx_messages_key_id ON messages(key, id)")
                                .fetch()
                                .rowsUpdated())
                .then()
                .block();
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
                messageRepository, handler, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE, 10, 5, 200);

        List<Long> first =
                messageRepository.claimBatch("w1", 10, 5).collectList().block();
        StepVerifier.create(Mono.just(first == null ? 0 : first.size()))
                .expectNextMatches(sz -> sz == 2)
                .verifyComplete();

        var list = messageRepository.fetchClaimedByIds(first).collectList().block();
        for (var row : list) {
            processRow(svc, row);
        }

        List<Long> second =
                messageRepository.claimBatch("w1", 10, 5).collectList().block();
        StepVerifier.create(Mono.just(second == null ? 0 : second.size()))
                .expectNext(1)
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

        ReactiveHeartbeatService hb = new ReactiveHeartbeatService(maintenanceRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE, 10_000);
        hb.heartbeat();

        ReactiveOrphanReclaimer rec = new ReactiveOrphanReclaimer(maintenanceRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE, 120_000, 10_000);
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

        Mono<List<Long>> c1 =
                messageRepository.claimBatch("w1", 10, 5).collectList().subscribeOn(Schedulers.parallel());
        Mono<List<Long>> c2 =
                messageRepository.claimBatch("w2", 10, 5).collectList().subscribeOn(Schedulers.parallel());

        var result = Mono.zip(c1, c2).block();
        assertThat(result).isNotNull();

        List<Long> first = result.getT1();
        List<Long> second = result.getT2();

        var overlap = new HashSet<>(first);
        overlap.retainAll(second);
        assertThat(overlap).isEmpty();

        var allClaimed = new HashSet<Long>();
        allClaimed.addAll(first);
        allClaimed.addAll(second);
        assertThat(allClaimed).hasSize(2);
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
