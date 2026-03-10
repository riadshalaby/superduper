package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;

class R2dbcWorkerMessageRepositoryIntegrationTest {

    static PostgreSQLContainer postgres;
    static DatabaseClient db;
    static R2dbcWorkerMessageRepository repo;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(postgres.getHost())
                .port(postgres.getMappedPort(5432))
                .database(postgres.getDatabaseName())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        repo = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.POSTGRES);
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void claimBatch_respectsPerKeyOrdering() {
        resetData();
        insert("u1", "k1", "v1", "READY");
        insert("u2", "k1", "v2", "READY");
        insert("u3", "k2", "v3", "READY");

        Long first = repo.claimBatch("w1", 10, 5).block();
        assertThat(first).isEqualTo(3L);

        Long second = repo.claimBatch("w1", 10, 5).block();
        assertThat(second).isZero();

        var firstRows = repo.fetchClaimedForWorker("w1").collectList().block();
        assertThat(firstRows).isNotNull();
        assertThat(firstRows).hasSize(3);
        firstRows.forEach(
                row -> assertThat(repo.markProcessed(row.id(), "w1").block()).isTrue());
        Integer processedAtCount = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSED' "
                        + "AND container_id='w1' AND processed_at IS NOT NULL")
                .map((row, md) -> row.get("c", Integer.class))
                .one()
                .block();
        assertThat(processedAtCount).isEqualTo(3);
        Long third = repo.claimBatch("w1", 10, 5).block();
        assertThat(third).isZero();
    }

    @Test
    void fetchAndFailureTransitions_work() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        Long claimed = repo.claimBatch("w1", 10, 5).block();
        assertThat(claimed).isEqualTo(1L);

        var rows = repo.fetchClaimedForWorker("w1").collectList().block();
        assertThat(rows).hasSize(1);

        long id = rows.getFirst().id();
        Boolean failed = repo.markFailed(id, 1, "w1").block();
        assertThat(failed).isTrue();
        String failedStatus = db.sql("SELECT status FROM messages WHERE id=" + id)
                .map((r, m) -> r.get("status", String.class))
                .one()
                .block();
        assertThat(failedStatus).isEqualTo("FAILED");
        Integer retry = db.sql("SELECT retry_count AS c FROM messages WHERE id=" + id)
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
        assertThat(retry).isEqualTo(1);

        Long reclaimed = repo.claimBatch("w2", 10, 5).block();
        assertThat(reclaimed).isEqualTo(1L);

        Boolean staleUpdateAccepted = repo.markStopped(id, 2, "w1").block();
        assertThat(staleUpdateAccepted).isFalse();

        Boolean stopByOwner = repo.markStopped(id, 2, "w2").block();
        assertThat(stopByOwner).isTrue();
        String status = db.sql("SELECT status FROM messages WHERE id=" + id)
                .map((r, m) -> r.get("status", String.class))
                .one()
                .block();
        assertThat(status).isEqualTo("STOPPED");
    }

    @Test
    void claimBatch_isolatesKeysPerTopic() {
        resetData();
        insert("a1", "shared-key", "va1", "READY", "topic-a");
        insert("a2", "shared-key", "va2", "READY", "topic-a");
        insert("b1", "shared-key", "vb1", "READY", "topic-b");
        insert("b2", "shared-key", "vb2", "READY", "topic-b");

        Long claimedTopicA = repo.claimBatch("w-topic-a", 10, 5, "topic-a").block();
        Long claimedTopicB = repo.claimBatch("w-topic-b", 10, 5, "topic-b").block();

        assertThat(claimedTopicA).isEqualTo(2);
        assertThat(claimedTopicB).isEqualTo(2);
        assertThat(repo.fetchClaimedForWorker("w-topic-a", "topic-a")
                        .collectList()
                        .block())
                .extracting(row -> row.topic() + ":" + row.messageId())
                .containsExactly("topic-a:a1", "topic-a:a2");
        assertThat(repo.fetchClaimedForWorker("w-topic-b", "topic-b")
                        .collectList()
                        .block())
                .extracting(row -> row.topic() + ":" + row.messageId())
                .containsExactly("topic-b:b1", "topic-b:b2");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
    }

    private static void insert(String messageId, String messageKey, String content, String status) {
        db.sql(
                        "INSERT INTO messages(message_id,message_key,content,status) VALUES (:messageId,:messageKey,:content,:status)")
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", content)
                .bind("status", status)
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static void insert(String messageId, String messageKey, String content, String status, String topic) {
        db.sql(
                        "INSERT INTO messages(message_id,message_key,content,status,topic) VALUES (:messageId,:messageKey,:content,:status,:topic)")
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", content)
                .bind("status", status)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .block();
    }
}
