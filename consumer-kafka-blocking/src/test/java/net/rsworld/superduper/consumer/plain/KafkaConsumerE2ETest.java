package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.JdbcMessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.SqlDialect;
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
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.annotation.EnableKafka;
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
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "spring");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-jdbc");
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
                "CREATE TABLE IF NOT EXISTS messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), timestamp TIMESTAMP, last_updated TIMESTAMP DEFAULT NOW())");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id)");
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
            prod.send(new ProducerRecord<>(TOPIC, "k-e2e", "hello-jdbc")).get();
        }

        var ds = DataSourceBuilder.create()
                .url(pg.getJdbcUrl())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        Integer found = 0;
        while (System.currentTimeMillis() < deadline) {
            found = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM messages WHERE key='k-e2e' AND content='hello-jdbc'", Integer.class);
            if (found != null && found > 0) break;
            Thread.sleep(250);
        }
        assertThat(found).isNotNull();
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
