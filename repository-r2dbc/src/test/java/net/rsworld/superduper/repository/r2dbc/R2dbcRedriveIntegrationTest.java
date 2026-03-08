package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;

class R2dbcRedriveIntegrationTest {

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
    void findByStatus_returnsFailedRowsOrderedAndLimited() {
        resetData();
        long failed1 = insert("f-1", "k1", "v1", "FAILED", 2, null);
        insert("r-1", "k1", "v2", "READY", 0, null);
        long failed2 = insert("f-2", "k2", "v3", "FAILED", 1, null);
        insert("s-1", "k3", "v4", "STOPPED", 5, null);

        List<ClaimedMessage> rows = repo.findByStatus("FAILED", 2).collectList().block();

        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(failed1, failed2);
        assertThat(rows).extracting(ClaimedMessage::retryCount).containsExactly(2, 1);
    }

    @Test
    void findByStatus_returnsStoppedRowsOnly() {
        resetData();
        long stopped1 = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        insert("f-1", "k2", "v2", "FAILED", 1, null);
        long stopped2 = insert("s-2", "k3", "v3", "STOPPED", 6, null);

        List<ClaimedMessage> rows =
                repo.findByStatus("STOPPED", 10).collectList().block();

        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(stopped1, stopped2);
    }

    @Test
    void redriveById_transitionsFailedRowToReady() {
        resetData();
        long id = insert("f-1", "k1", "v1", "FAILED", 3, "worker-a");

        assertThat(repo.redriveById(id).block()).isEqualTo(1);
        assertThat(messageRow(id))
                .containsEntry("status", "READY")
                .containsEntry("retry_count", 0)
                .containsEntry("container_id", null);
    }

    @Test
    void redriveById_transitionsStoppedRowToReady() {
        resetData();
        long id = insert("s-1", "k1", "v1", "STOPPED", 5, "worker-a");

        assertThat(repo.redriveById(id).block()).isEqualTo(1);
        assertThat(messageRow(id))
                .containsEntry("status", "READY")
                .containsEntry("retry_count", 0)
                .containsEntry("container_id", null);
    }

    @Test
    void redriveById_isNoopForReadyAndProcessingRows() {
        resetData();
        long readyId = insert("r-1", "k1", "v1", "READY", 0, null);
        long processingId = insert("p-1", "k2", "v2", "PROCESSING", 1, "worker-a");

        assertThat(repo.redriveById(readyId).block()).isZero();
        assertThat(repo.redriveById(processingId).block()).isZero();
    }

    @Test
    void redriveByStatus_redrivesUpToLimit() {
        resetData();
        long id1 = insert("f-1", "k1", "v1", "FAILED", 2, null);
        long id2 = insert("f-2", "k2", "v2", "FAILED", 3, null);
        long id3 = insert("f-3", "k3", "v3", "FAILED", 4, null);

        assertThat(repo.redriveByStatus("FAILED", 2).block()).isEqualTo(2);

        assertThat(messageRow(id1)).containsEntry("status", "READY").containsEntry("retry_count", 0);
        assertThat(messageRow(id2)).containsEntry("status", "READY").containsEntry("retry_count", 0);
        assertThat(messageRow(id3)).containsEntry("status", "FAILED").containsEntry("retry_count", 4);
    }

    @Test
    void redrivenStoppedMessageWithReadySibling_isClaimedInIdOrder() {
        resetData();
        long stoppedId = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        long readyId = insert("r-1", "k1", "v2", "READY", 0, null);

        assertThat(repo.redriveById(stoppedId).block()).isEqualTo(1);
        assertThat(repo.claimBatch("w1", 10, 5).block()).isEqualTo(2L);

        List<ClaimedMessage> rows =
                repo.fetchClaimedForWorker("w1").collectList().block();
        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(stoppedId, readyId);
    }

    @Test
    void redrivenStoppedMessageWithProcessingSibling_waitsUntilSiblingCompletes() {
        resetData();
        long stoppedId = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        long processingId = insert("p-1", "k1", "v2", "PROCESSING", 1, "worker-other");

        assertThat(repo.redriveById(stoppedId).block()).isEqualTo(1);
        assertThat(repo.claimBatch("w1", 10, 5).block()).isZero();

        db.sql("UPDATE messages SET status='PROCESSED', processed_at=NOW() WHERE id=:id")
                .bind("id", processingId)
                .fetch()
                .rowsUpdated()
                .block();

        assertThat(repo.claimBatch("w1", 10, 5).block()).isEqualTo(1L);
        assertThat(repo.fetchClaimedForWorker("w1").collectList().block())
                .extracting(ClaimedMessage::id)
                .containsExactly(stoppedId);
    }

    @Test
    void countByStatus_returnsQueueCounts() {
        resetData();
        insert("r-1", "k1", "v1", "READY", 0, null);
        insert("f-1", "k2", "v2", "FAILED", 2, null);
        insert("s-1", "k3", "v3", "STOPPED", 5, null);
        insert("p-1", "k4", "v4", "PROCESSING", 1, "worker-a");

        assertThat(repo.countByStatus().block())
                .containsEntry("READY", 1L)
                .containsEntry("FAILED", 1L)
                .containsEntry("STOPPED", 1L)
                .containsEntry("PROCESSING", 1L);
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
    }

    private static long insert(
            String messageId, String messageKey, String content, String status, int retryCount, String containerId) {
        var statement = db.sql(
                        "INSERT INTO messages(message_id,message_key,content,status,retry_count,container_id,last_updated) "
                                + "VALUES (:messageId,:messageKey,:content,:status,:retryCount,:containerId,NOW())")
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", content)
                .bind("status", status)
                .bind("retryCount", retryCount);
        if (containerId == null) {
            statement = statement.bindNull("containerId", String.class);
        } else {
            statement = statement.bind("containerId", containerId);
        }
        statement.fetch().rowsUpdated().block();
        return db.sql("SELECT MAX(id) AS id FROM messages")
                .map((row, metadata) -> row.get("id", Long.class))
                .one()
                .block();
    }

    private static Map<String, Object> messageRow(long id) {
        return db.sql("SELECT status, retry_count, container_id FROM messages WHERE id=:id")
                .bind("id", id)
                .map((row, metadata) -> {
                    Map<String, Object> values = new LinkedHashMap<>();
                    values.put("status", row.get("status", String.class));
                    values.put("retry_count", row.get("retry_count", Integer.class));
                    values.put("container_id", row.get("container_id", String.class));
                    return values;
                })
                .one()
                .block();
    }
}
