package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.JdbcMessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.SqlDialect;
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
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(classes = KafkaConsumerE2ETest.TestConfig.class)
class KafkaConsumerE2ETest {

    static final String TOPIC = "e2e.topic.jdbc";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "spring");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-jdbc");
        r.add("superduper.kafka.topic", () -> TOPIC);
        r.add("superduper.consumer.max-poll-records", () -> 10);
        r.add("spring.datasource.url", pg::getJdbcUrl);
        r.add("spring.datasource.username", pg::getUsername);
        r.add("spring.datasource.password", pg::getPassword);
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

        var ds = DataSourceBuilder.create()
                .url(pg.getJdbcUrl())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("TRUNCATE TABLE messages RESTART IDENTITY");

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
        Integer found = 0;
        while (System.currentTimeMillis() < deadline) {
            found = jdbc.queryForObject("SELECT COUNT(*) FROM messages", Integer.class);
            if (found != null && found == 4 && TestConfig.maxBatchSize.get() >= 2) {
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
    @Import(KafkaConsumerAutoConfiguration.class)
    static class TestConfig {
        static final AtomicInteger batchCalls = new AtomicInteger();
        static final AtomicInteger maxBatchSize = new AtomicInteger();
        static final AtomicInteger singleCalls = new AtomicInteger();

        static void reset() {
            batchCalls.set(0);
            maxBatchSize.set(0);
            singleCalls.set(0);
        }

        @Bean
        DataSource dataSource() {
            return DataSourceBuilder.create()
                    .url(pg.getJdbcUrl())
                    .username(pg.getUsername())
                    .password(pg.getPassword())
                    .build();
        }

        @Bean
        NamedParameterJdbcTemplate npJdbc(DataSource ds) {
            return new NamedParameterJdbcTemplate(ds);
        }

        @Bean
        MessageIngestRepository messageIngestRepository(NamedParameterJdbcTemplate np) {
            return new TrackingJdbcMessageIngestRepository(np, SqlDialect.POSTGRES);
        }
    }

    static class TrackingJdbcMessageIngestRepository extends JdbcMessageIngestRepository {
        TrackingJdbcMessageIngestRepository(NamedParameterJdbcTemplate jdbc, SqlDialect dialect) {
            super(jdbc, dialect);
        }

        @Override
        public void batchUpsertReadyMessages(List<MessageIngestData> messages) {
            TestConfig.batchCalls.incrementAndGet();
            TestConfig.maxBatchSize.accumulateAndGet(messages.size(), Math::max);
            super.batchUpsertReadyMessages(messages);
        }

        @Override
        public void upsertReadyMessage(
                String topic,
                String messageId,
                String messageKey,
                String content,
                Instant occurredAt,
                String correlationId,
                String messageType) {
            TestConfig.singleCalls.incrementAndGet();
            super.upsertReadyMessage(topic, messageId, messageKey, content, occurredAt, correlationId, messageType);
        }
    }
}
