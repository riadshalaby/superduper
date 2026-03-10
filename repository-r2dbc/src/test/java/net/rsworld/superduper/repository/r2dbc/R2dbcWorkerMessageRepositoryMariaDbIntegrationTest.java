package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.mariadb.MariaDBContainer;

class R2dbcWorkerMessageRepositoryMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static DatabaseClient db;
    static R2dbcWorkerMessageRepository repo;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());

        ConnectionFactory cf = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "mariadb")
                .option(ConnectionFactoryOptions.HOST, mariadb.getHost())
                .option(ConnectionFactoryOptions.PORT, mariadb.getMappedPort(3306))
                .option(ConnectionFactoryOptions.DATABASE, mariadb.getDatabaseName())
                .option(ConnectionFactoryOptions.USER, mariadb.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, mariadb.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        repo = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.MARIADB);
    }

    @AfterAll
    static void afterAll() {
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @Test
    void claimBatch_respectsPerKeyOrdering_onMariaDb() {
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
        firstRows.forEach(
                row -> assertThat(repo.markProcessed(row.id(), "w1").block()).isTrue());
        Long third = repo.claimBatch("w1", 10, 5).block();
        assertThat(third).isZero();
    }

    @Test
    void claimBatch_isolatesKeysPerTopic_onMariaDb() {
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
        db.sql("TRUNCATE TABLE messages").fetch().rowsUpdated().block();
    }

    private static void insert(String messageId, String messageKey, String content, String status) {
        db.sql("INSERT INTO messages(message_id,message_key,content,status) "
                        + "VALUES (:messageId,:messageKey,:content,:status)")
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", content)
                .bind("status", status)
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static void insert(String messageId, String messageKey, String content, String status, String topic) {
        db.sql("INSERT INTO messages(message_id,message_key,content,status,topic) "
                        + "VALUES (:messageId,:messageKey,:content,:status,:topic)")
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
