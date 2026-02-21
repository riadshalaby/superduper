package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.SqlDialect;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

@Testcontainers
@SpringBootTest(classes = KafkaReactorE2ETest.TestConfig.class)
class KafkaReactorE2ETest {

    static final String TOPIC = "e2e.topic.reactor";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "reactor");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-reactor");
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
                        "CREATE TABLE IF NOT EXISTS messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())")
                .fetch()
                .rowsUpdated()
                .block();
        db.sql("CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id)")
                .fetch()
                .rowsUpdated()
                .block();
    }

    @AfterAll
    static void shutdown() {}

    @Test
    void endToEnd_insertOnConsume() throws Exception {
        ensureTopic(kafka.getBootstrapServers(), TOPIC);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            prod.send(new ProducerRecord<>(TOPIC, "k-e2e-reactive", "hello-reactor"))
                    .get();
        }

        var cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        DatabaseClient db = DatabaseClient.create(cf);

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        int found = 0;
        while (System.currentTimeMillis() < deadline) {
            Integer cnt = db.sql(
                            "SELECT COUNT(*) AS c FROM messages WHERE key='k-e2e-reactive' AND content='hello-reactor'")
                    .map((row, meta) -> row.get("c", Integer.class))
                    .one()
                    .onErrorResume(e -> Mono.just(0))
                    .block();
            if (cnt != null && cnt > 0) {
                found = cnt;
                break;
            }
            Thread.sleep(250);
        }
        assertThat(found).isGreaterThan(0);
    }

    private static void ensureTopic(String bootstrapServers, String topic) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();

            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }

            while (System.currentTimeMillis() < deadline) {
                try {
                    var desc =
                            admin.describeTopics(List.of(topic)).allTopicNames().get();
                    var partitions = desc.get(topic).partitions();
                    boolean leaderReady = partitions.stream()
                            .allMatch(p -> p.leader() != null && p.leader().id() >= 0);
                    if (leaderReady) {
                        return;
                    }
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw e;
                    }
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("Topic leader not ready for topic " + topic);
        }
    }

    @Configuration
    @EnableKafka
    @Import(KafkaReactorR2dbcAutoConfiguration.class)
    static class TestConfig {
        @Bean
        DatabaseClient databaseClient() {
            var cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                    .host(pg.getHost())
                    .port(pg.getMappedPort(5432))
                    .database(pg.getDatabaseName())
                    .username(pg.getUsername())
                    .password(pg.getPassword())
                    .build());
            return DatabaseClient.create(cf);
        }

        @Bean
        ReactiveMessageIngestRepository reactiveMessageIngestRepository(DatabaseClient db) {
            return new R2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);
        }
    }
}
