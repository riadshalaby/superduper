package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Properties;
import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.task.DefaultLockingTaskExecutor;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.MessageRow;
import net.rsworld.superduper.worker.jdbc.ProcessingResult;
import net.rsworld.superduper.worker.jdbc.SuperDuperWorkerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(classes = KafkaConsumerWorkerE2EIT.TestConfig.class)
class KafkaConsumerWorkerE2EIT {

    static final String TOPIC = "e2e.worker.jdbc";

    @Container
    static KafkaContainer kafka = new KafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

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
                "CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())");
        jdbc.execute(
                "CREATE TABLE IF NOT EXISTS container_heartbeats (container_id VARCHAR(255) PRIMARY KEY, last_heartbeat TIMESTAMP DEFAULT NOW())");
        jdbc.execute(
                "CREATE TABLE IF NOT EXISTS shedlock (name VARCHAR(64) PRIMARY KEY, lock_until TIMESTAMP(3) NOT NULL, locked_at TIMESTAMP(3) NOT NULL, locked_by VARCHAR(255) NOT NULL)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id)");
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

        SuperDuperWorkerService svc = new SuperDuperWorkerService(np, txm, lockExec, handler, 10, 5);

        var claim = SuperDuperWorkerService.class.getDeclaredMethod("claimBatch");
        claim.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<Integer> first = (java.util.List<Integer>) claim.invoke(svc);
        assertThat(first).hasSize(2);
        var process = SuperDuperWorkerService.class.getDeclaredMethod("process", java.util.List.class);
        process.setAccessible(true);
        process.invoke(svc, first);

        @SuppressWarnings("unchecked")
        java.util.List<Integer> second = (java.util.List<Integer>) claim.invoke(svc);
        assertThat(second).hasSize(1);
        process.invoke(svc, second);

        Integer processed =
                jdbc.queryForObject("SELECT COUNT(*) FROM messages WHERE status='PROCESSED'", Integer.class);
        assertThat(processed).isEqualTo(3);
    }

    @Configuration
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
    }
}
