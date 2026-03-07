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
}
