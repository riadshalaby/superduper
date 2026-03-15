package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.SqlDialect;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
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
@SpringBootTest(classes = KafkaReactiveE2ETest.TestConfig.class)
class KafkaReactiveE2ETest {

    static final String TOPIC = "e2e.topic.reactor";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "reactor");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-reactor");
        r.add("superduper.kafka.topic", () -> TOPIC);
        r.add("superduper.consumer.max-poll-records", () -> 10);
    }

    @BeforeAll
    static void initSchema() {
        LiquibaseTestSupport.migrate(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }

    @AfterAll
    static void shutdown() {}

    @Test
    void endToEnd_batchInsertAndDedupByMessageId() throws Exception {
        ensureTopic(kafka.getBootstrapServers(), TOPIC);
        stopContainers();
        TestConfig.reset();

        DatabaseClient db = TestConfig.databaseClient();
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            prod.send(record("k-1", "hello-1", "msg-1")).get();
            prod.send(record("k-2", "hello-2", "msg-2")).get();
            prod.send(record("k-3", "hello-3", "dup-1")).get();
            prod.send(record("k-4", "hello-4", "dup-1")).get();
            prod.send(record("k-5", "hello-5", "msg-5")).get();
        }

        startContainers();

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        int found = 0;
        while (System.currentTimeMillis() < deadline) {
            Integer count = db.sql("SELECT COUNT(*) AS c FROM messages")
                    .map((row, md) -> row.get("c", Integer.class))
                    .one()
                    .block();
            if (count != null) {
                found = count;
            }
            if (found == 4 && TestConfig.maxBatchSize.get() >= 2) {
                break;
            }
            pauseMillis(250);
        }

        assertThat(found).isEqualTo(4);
        assertThat(TestConfig.batchCalls.get()).isGreaterThan(0);
        assertThat(TestConfig.maxBatchSize.get()).isGreaterThanOrEqualTo(2);
        assertThat(TestConfig.singleCalls.get()).isEqualTo(0);
    }

    private void stopContainers() {
        registry.getListenerContainers().forEach(MessageListenerContainer::stop);
    }

    private void startContainers() {
        registry.getListenerContainers().forEach(MessageListenerContainer::start);
    }

    private static ProducerRecord<String, String> record(String key, String value, String messageId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
        record.headers()
                .add(new RecordHeader("message_id", messageId.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        record.headers()
                .add(new RecordHeader(
                        "occurred_at",
                        Instant.parse("2026-02-21T10:15:30Z")
                                .toString()
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return record;
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
                pauseMillis(200);
            }
            throw new IllegalStateException("Topic leader not ready for topic " + topic);
        }
    }

    private static void pauseMillis(long millis) {
        LockSupport.parkNanos(Duration.ofMillis(millis).toNanos());
    }

    @Configuration
    @EnableKafka
    @Import(KafkaReactiveR2dbcAutoConfiguration.class)
    static class TestConfig {
        static final AtomicInteger batchCalls = new AtomicInteger();
        static final AtomicInteger maxBatchSize = new AtomicInteger();
        static final AtomicInteger singleCalls = new AtomicInteger();

        static void reset() {
            batchCalls.set(0);
            maxBatchSize.set(0);
            singleCalls.set(0);
        }

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

        @Bean
        DatabaseClient databaseClientBean() {
            return databaseClient();
        }

        @Bean
        ReactiveMessageIngestRepository reactiveMessageIngestRepository(DatabaseClient db) {
            return new TrackingR2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);
        }
    }

    static class TrackingR2dbcMessageIngestRepository extends R2dbcMessageIngestRepository {
        TrackingR2dbcMessageIngestRepository(DatabaseClient db, SqlDialect dialect) {
            super(db, dialect);
        }

        @Override
        public Mono<Void> batchUpsertReadyMessages(List<MessageIngestData> messages) {
            TestConfig.batchCalls.incrementAndGet();
            TestConfig.maxBatchSize.accumulateAndGet(messages.size(), Math::max);
            return super.batchUpsertReadyMessages(messages);
        }

        @Override
        public Mono<Void> upsertReadyMessage(
                String topic,
                String messageId,
                String messageKey,
                String content,
                Instant occurredAt,
                String correlationId,
                String messageType) {
            TestConfig.singleCalls.incrementAndGet();
            return super.upsertReadyMessage(
                    topic, messageId, messageKey, content, occurredAt, correlationId, messageType);
        }
    }
}
