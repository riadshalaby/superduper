package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.JdbcMessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.SqlDialect;
import net.rsworld.superduper.worker.blocking.MessageHandler;
import net.rsworld.superduper.worker.blocking.MessageRow;
import net.rsworld.superduper.worker.blocking.ProcessingResult;
import net.rsworld.superduper.worker.blocking.SuperDuperWorkerService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(classes = KafkaConsumerWorkerE2EIT.TestConfig.class)
class KafkaConsumerWorkerE2EIT {

    static final String TOPIC = "e2e.worker.blocking";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "spring");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-jdbc-worker");
        r.add("superduper.kafka.topic", () -> TOPIC);
        r.add("spring.datasource.url", pg::getJdbcUrl);
        r.add("spring.datasource.username", pg::getUsername);
        r.add("spring.datasource.password", pg::getPassword);
    }

    @BeforeAll
    static void initSchema() {
        var ds = DataSourceBuilder.create()
                .url(pg.getJdbcUrl())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute(
                "CREATE TABLE IF NOT EXISTS messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())");
        jdbc.execute(
                "CREATE TABLE IF NOT EXISTS container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())");
        jdbc.execute(
                "CREATE TABLE IF NOT EXISTS shedlock (name VARCHAR(64) PRIMARY KEY, lock_until TIMESTAMP(3) NOT NULL, locked_at TIMESTAMP(3) NOT NULL, locked_by VARCHAR(255) NOT NULL)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id)");
    }

    @Test
    void full_flow_consume_persist_claim_process_in_order() throws Exception {
        ensureTopic(kafka.getBootstrapServers(), TOPIC);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            prod.send(new ProducerRecord<>(TOPIC, "k1", "a")).get();
            prod.send(new ProducerRecord<>(TOPIC, "k1", "b")).get();
            prod.send(new ProducerRecord<>(TOPIC, "k2", "c")).get();
        }

        var ds = DataSourceBuilder.create()
                .url(pg.getJdbcUrl())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        int count = 0;
        while (System.currentTimeMillis() < deadline) {
            count = jdbc.queryForObject("SELECT COUNT(*) FROM messages", Integer.class);
            if (count >= 3) break;
            Thread.sleep(200);
        }
        assertThat(count).isGreaterThanOrEqualTo(3);

        NamedParameterJdbcTemplate np = new NamedParameterJdbcTemplate(ds);
        DataSourceTransactionManager txm = new DataSourceTransactionManager(ds);

        LockProvider lockProvider = new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withTableName("shedlock")
                .withJdbcTemplate(new JdbcTemplate(ds))
                .build());
        LockingTaskExecutor lockExec = new DefaultLockingTaskExecutor(lockProvider);

        MessageHandler handler = (MessageRow r) -> ProcessingResult.SUCCESS;
        WorkerMessageRepository messageRepository = new JdbcWorkerMessageRepository(np, SqlDialect.POSTGRES);

        SuperDuperWorkerService svc = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);

        var claim = SuperDuperWorkerService.class.getDeclaredMethod("claimBatch");
        claim.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<Long> first = (java.util.List<Long>) claim.invoke(svc);
        assertThat(first).hasSize(2);
        var process = SuperDuperWorkerService.class.getDeclaredMethod("process", java.util.List.class);
        process.setAccessible(true);
        process.invoke(svc, first);

        @SuppressWarnings("unchecked")
        java.util.List<Long> second = (java.util.List<Long>) claim.invoke(svc);
        assertThat(second).hasSize(1);
        process.invoke(svc, second);

        Integer processed =
                jdbc.queryForObject("SELECT COUNT(*) FROM messages WHERE status='PROCESSED'", Integer.class);
        assertThat(processed).isEqualTo(3);
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
    @Import(KafkaConsumerAutoConfiguration.class)
    static class TestConfig {
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
            return new JdbcMessageIngestRepository(np, SqlDialect.POSTGRES);
        }
    }
}
