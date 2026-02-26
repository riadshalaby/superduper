package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.SqlDialect;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import net.rsworld.superduper.worker.reactive.ProcessingResult;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

@Testcontainers
@SpringBootTest(classes = KafkaReactiveWorkerE2EIT.TestConfig.class)
class KafkaReactiveWorkerE2EIT {

    static final String TOPIC = "e2e.worker.reactive";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    @Container
    static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("superduper.consumer.type", () -> "reactor");
        r.add("superduper.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("superduper.kafka.group-id", () -> "e2e-reactor-worker");
        r.add("superduper.kafka.topic", () -> TOPIC);
    }

    @BeforeAll
    static void initSchema() {
        LiquibaseTestSupport.migrate(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
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
        var cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(pg.getHost())
                .port(pg.getMappedPort(5432))
                .database(pg.getDatabaseName())
                .username(pg.getUsername())
                .password(pg.getPassword())
                .build());
        ReactiveWorkerMessageRepository messageRepository = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.POSTGRES);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                messageRepository,
                org.mockito.Mockito.mock(LockingTaskExecutor.class),
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                10,
                5);

        Long first = messageRepository.claimBatch("w1", 10, 5).block();
        assertThat(first).isEqualTo(2L);

        var list = messageRepository.fetchClaimedForWorker("w1").collectList().block();
        for (var row : list) {
            var processOne = svc.getClass()
                    .getDeclaredMethod("processOne", net.rsworld.superduper.repository.api.ClaimedMessage.class);
            processOne.setAccessible(true);
            ((reactor.core.publisher.Mono<Void>) processOne.invoke(svc, row)).block();
        }

        Long second = messageRepository.claimBatch("w1", 10, 5).block();
        assertThat(second).isEqualTo(1L);
        list = messageRepository.fetchClaimedForWorker("w1").collectList().block();
        for (var row : list) {
            var processOne = svc.getClass()
                    .getDeclaredMethod("processOne", net.rsworld.superduper.repository.api.ClaimedMessage.class);
            processOne.setAccessible(true);
            ((reactor.core.publisher.Mono<Void>) processOne.invoke(svc, row)).block();
        }

        Integer processed = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSED'")
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
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
    @Import(KafkaReactiveR2dbcAutoConfiguration.class)
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

        @Bean
        ReactiveMessageIngestRepository reactiveMessageIngestRepository(DatabaseClient db) {
            return new R2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);
        }
    }
}
