package net.rsworld.superduper.worker.reactive;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers
class WorkerReactiveIntegrationTest {

    static PostgreSQLContainer<?> pg;
    static DatabaseClient db;

    @BeforeAll
    static void setup() {
        pg = new PostgreSQLContainer<>("postgres:16-alpine");
        pg.start();

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        db = DatabaseClient.create(cf);

        Flux.concat(
                        db.sql(
                                        "CREATE TABLE messages (id SERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())")
                                .fetch()
                                .rowsUpdated(),
                        db.sql(
                                        "CREATE TABLE container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("CREATE INDEX idx_messages_key_id ON messages(key, id)")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("INSERT INTO messages(uuid,key,content,status) VALUES ('ru1','k1','v1','READY')")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("INSERT INTO messages(uuid,key,content,status) VALUES ('ru2','k1','v2','READY')")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("INSERT INTO messages(uuid,key,content,status) VALUES ('ru3','k2','v3','READY')")
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
        ReactiveMessageHandler handler = r -> Mono.just(ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc =
                new SuperDuperWorkerReactiveService(db, handler, 10, 5, 200, 10_000, 120_000);

        try {
            var claim = svc.getClass().getDeclaredMethod("claimBatch");
            claim.setAccessible(true);
            @SuppressWarnings("unchecked")
            reactor.core.publisher.Flux<Integer> flux = (reactor.core.publisher.Flux<Integer>) claim.invoke(svc);
            java.util.List<Integer> first = flux.collectList().block();
            StepVerifier.create(Mono.just(first.size()))
                    .expectNextMatches(sz -> sz == 2)
                    .verifyComplete();

            var fetch = svc.getClass().getDeclaredMethod("fetchClaimed", java.util.List.class);
            fetch.setAccessible(true);
            @SuppressWarnings("unchecked")
            reactor.core.publisher.Flux<java.util.Map<String, Object>> rows =
                    (reactor.core.publisher.Flux<java.util.Map<String, Object>>) fetch.invoke(svc, first);
            java.util.List<java.util.Map<String, Object>> list =
                    rows.collectList().block();

            for (var row : list) {
                var processOne = svc.getClass().getDeclaredMethod("processOne", java.util.Map.class);
                processOne.setAccessible(true);
                ((reactor.core.publisher.Mono<Void>) processOne.invoke(svc, row)).block();
            }

            @SuppressWarnings("unchecked")
            java.util.List<Integer> second = ((reactor.core.publisher.Flux<Integer>) claim.invoke(svc))
                    .collectList()
                    .block();
            StepVerifier.create(Mono.just(second.size())).expectNext(1).verifyComplete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void reactive_heartbeat_and_orphan_reclaimer() {
        db.sql("UPDATE messages SET status='PROCESSING', last_updated = NOW() - INTERVAL '10 minutes' WHERE uuid='ru3'")
                .fetch()
                .rowsUpdated()
                .block();

        ReactiveHeartbeatService hb = new ReactiveHeartbeatService(db, 10_000);
        hb.heartbeat();

        ReactiveOrphanReclaimer rec = new ReactiveOrphanReclaimer(db, 120_000, 10_000);
        rec.reclaim();

        Mono<String> status = db.sql("SELECT status FROM messages WHERE uuid='ru3'")
                .map((r, m) -> r.get("status", String.class))
                .one();
        StepVerifier.create(status).expectNext("READY").verifyComplete();
    }
}
