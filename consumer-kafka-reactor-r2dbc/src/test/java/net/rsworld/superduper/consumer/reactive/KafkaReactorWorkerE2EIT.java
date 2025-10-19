package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import net.rsworld.superduper.worker.reactive.ProcessingResult;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@Testcontainers
@SpringBootTest(classes = KafkaReactorWorkerE2EIT.TestConfig.class)
class KafkaReactorWorkerE2EIT {

    static final String TOPIC = "e2e.worker.reactive";

    @Container
    static KafkaContainer kafka = new KafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "reactor");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-reactor-worker");
        r.add("superduper.kafka.topic", () -> TOPIC);
    }

    @BeforeAll
    static void initSchema() {
        var cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        DatabaseClient db = DatabaseClient.create(cf);
        db.sql(
                        "CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())")
                .fetch()
                .rowsUpdated()
                .block();
        db.sql("CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id)")
                .fetch()
                .rowsUpdated()
                .block();
        db.sql(
                        "CREATE TABLE IF NOT EXISTS container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())")
                .fetch()
                .rowsUpdated()
                .block();
    }

    @Test
    void full_flow_consume_persist_claim_process_in_order() throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            prod.send(new ProducerRecord<>(TOPIC, "k1", "a")).get();
            prod.send(new ProducerRecord<>(TOPIC, "k1", "b")).get();
            prod.send(new ProducerRecord<>(TOPIC, "k2", "c")).get();
        }

        DatabaseClient db = TestConfig.databaseClient();
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        int count = 0;
        while (System.currentTimeMillis() < deadline) {
            Integer c = db.sql("SELECT COUNT(*) AS c FROM messages")
                    .map((r, m) -> r.get("c", Integer.class))
                    .one()
                    .onErrorResume(e -> Mono.just(0))
                    .block();
            if (c != null && c >= 3) {
                count = c;
                break;
            }
            Thread.sleep(200);
        }
        assertThat(count).isGreaterThanOrEqualTo(3);

        ReactiveMessageHandler handler = row -> Mono.just(ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc =
                new SuperDuperWorkerReactiveService(db, handler, 10, 5, 500, 10_000, 120_000);

        var claim = svc.getClass().getDeclaredMethod("claimBatch");
        claim.setAccessible(true);
        @SuppressWarnings("unchecked")
        reactor.core.publisher.Flux<Integer> flux = (reactor.core.publisher.Flux<Integer>) claim.invoke(svc);
        List<Integer> first = flux.collectList().block();
        assertThat(first).hasSize(2);

        var fetch = svc.getClass().getDeclaredMethod("fetchClaimed", java.util.List.class);
        fetch.setAccessible(true);
        @SuppressWarnings("unchecked")
        reactor.core.publisher.Flux<java.util.Map<String, Object>> rows =
                (reactor.core.publisher.Flux<java.util.Map<String, Object>>) fetch.invoke(svc, first);
        var list = rows.collectList().block();
        for (var row : list) {
            var processOne = svc.getClass().getDeclaredMethod("processOne", java.util.Map.class);
            processOne.setAccessible(true);
            ((reactor.core.publisher.Mono<Void>) processOne.invoke(svc, row)).block();
        }

        List<Integer> second = ((reactor.core.publisher.Flux<Integer>) claim.invoke(svc))
                .collectList()
                .block();
        assertThat(second).hasSize(1);
        rows = (reactor.core.publisher.Flux<java.util.Map<String, Object>>) fetch.invoke(svc, second);
        list = rows.collectList().block();
        for (var row : list) {
            var processOne = svc.getClass().getDeclaredMethod("processOne", java.util.Map.class);
            processOne.setAccessible(true);
            ((reactor.core.publisher.Mono<Void>) processOne.invoke(svc, row)).block();
        }

        Integer processed = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSED'")
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
        assertThat(processed).isEqualTo(3);
    }

    @Configuration
    @Import({KafkaReactorR2dbcAutoConfiguration.class, KafkaReactorR2dbcConsumerService.class})
    static class TestConfig {
        @Bean
        static DatabaseClient databaseClient() {
            var cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                    .host(pg.getHost())
                    .port(pg.getMappedPort(5432))
                    .database(pg.getDatabaseName())
                    .username(pg.getUsername())
                    .password(pg.getPassword())
                    .build());
            return DatabaseClient.create(cf);
        }
    }
}
