package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.util.List;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;

class R2dbcCleanupIntegrationTest {

    static PostgreSQLContainer postgres;
    static DatabaseClient db;
    static R2dbcWorkerMaintenanceRepository maintenanceRepository;
    static R2dbcWorkerMessageRepository messageRepository;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        PostgresqlConnectionFactory connectionFactory =
                new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                        .host(postgres.getHost())
                        .port(postgres.getMappedPort(5432))
                        .database(postgres.getDatabaseName())
                        .username(postgres.getUsername())
                        .password(postgres.getPassword())
                        .build());
        db = DatabaseClient.create(connectionFactory);
        maintenanceRepository = new R2dbcWorkerMaintenanceRepository(db, R2dbcSqlDialects.from(SqlDialect.POSTGRES));
        messageRepository = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(connectionFactory)), SqlDialect.POSTGRES);
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void deleteProcessedOlderThan_removesOnlyExpiredProcessedRows() {
        resetData();
        long deletedId = insertMessage("processed-old", "k1", "PROCESSED", 20, 0, null);
        long retainedId = insertMessage("processed-new", "k2", "PROCESSED", 4, 0, null);
        long readyId = insertMessage("ready-old", "k3", "READY", 25, 0, null);

        assertThat(maintenanceRepository.deleteProcessedOlderThan(14).block()).isEqualTo(1);
        assertThat(messageExists(deletedId)).isFalse();
        assertThat(messageExists(retainedId)).isTrue();
        assertThat(messageStatus(readyId)).isEqualTo("READY");
    }

    @Test
    void deleteStoppedOlderThan_preservesRecentStoppedRowsForRedrive() {
        resetData();
        long deletedId = insertMessage("stopped-old", "k1", "STOPPED", 40, 5, null);
        long retainedId = insertMessage("stopped-new", "k2", "STOPPED", 5, 5, null);
        insertMessage("failed-old", "k3", "FAILED", 40, 2, null);

        assertThat(maintenanceRepository.deleteStoppedOlderThan(30).block()).isEqualTo(1);
        assertThat(messageExists(deletedId)).isFalse();
        assertThat(messageExists(retainedId)).isTrue();
        assertThat(messageRepository.redriveByStatus("STOPPED", 10).block()).isEqualTo(1);
        assertThat(messageStatus(retainedId)).isEqualTo("READY");
    }

    @Test
    void deleteStaleHeartbeats_removesOnlyExpiredRows() {
        resetData();
        insertHeartbeat("worker-old", 2);
        insertHeartbeat("worker-new", 0);

        assertThat(maintenanceRepository.deleteStaleHeartbeats(1).block()).isEqualTo(1);
        assertThat(heartbeatExists("worker-old")).isFalse();
        assertThat(heartbeatExists("worker-new")).isTrue();
    }

    @Test
    void cleanup_doesNotInterfereWithClaimOrOrphanRecovery() {
        resetData();
        long readyId = insertMessage("ready-old", "k1", "READY", 45, 0, null);
        long failedId = insertMessage("failed-old", "k2", "FAILED", 45, 1, null);
        insertMessage("processed-old", "k3", "PROCESSED", 45, 0, null);
        insertMessage("stopped-old", "k4", "STOPPED", 45, 5, null);
        long processingId = insertMessage("processing-old", "k5", "PROCESSING", 45, 1, "worker-old");

        assertThat(maintenanceRepository.deleteProcessedOlderThan(14).block()).isEqualTo(1);
        assertThat(maintenanceRepository.deleteStoppedOlderThan(30).block()).isEqualTo(1);

        assertThat(messageRepository.claimBatch("worker-a", 10, 5).block()).isEqualTo(2L);
        List<ClaimedMessage> claimed = messageRepository
                .fetchClaimedForWorker("worker-a")
                .collectList()
                .block();
        assertThat(claimed).extracting(ClaimedMessage::id).containsExactly(readyId, failedId);
        assertThat(messageStatus(processingId)).isEqualTo("PROCESSING");

        assertThat(maintenanceRepository.reclaimStaleProcessing(1).block()).isEqualTo(1);
        assertThat(messageStatus(processingId)).isEqualTo("READY");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE container_heartbeats").fetch().rowsUpdated().block();
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
    }

    private static long insertMessage(
            String messageId, String messageKey, String status, int ageDays, int retryCount, String containerId) {
        var statement = db.sql(
                        "INSERT INTO messages(message_id, message_key, content, status, retry_count, container_id, last_updated) "
                                + "VALUES (:messageId, :messageKey, :content, :status, :retryCount, :containerId, "
                                + "(NOW() - (:ageDays * INTERVAL '1 day')))")
                .bind("messageId", messageId)
                .bind("messageKey", messageKey)
                .bind("content", "payload-" + messageId)
                .bind("status", status)
                .bind("retryCount", retryCount)
                .bind("ageDays", ageDays);
        statement = containerId == null
                ? statement.bindNull("containerId", String.class)
                : statement.bind("containerId", containerId);
        statement.fetch().rowsUpdated().block();
        return db.sql("SELECT MAX(id) AS id FROM messages")
                .map((row, metadata) -> row.get("id", Long.class))
                .one()
                .block();
    }

    private static void insertHeartbeat(String containerId, int ageDays) {
        db.sql("INSERT INTO container_heartbeats(container_id, last_heartbeat) "
                        + "VALUES (:containerId, (NOW() - (:ageDays * INTERVAL '1 day')))")
                .bind("containerId", containerId)
                .bind("ageDays", ageDays)
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static boolean messageExists(long id) {
        Integer count = db.sql("SELECT COUNT(*) AS cnt FROM messages WHERE id=:id")
                .bind("id", id)
                .map((row, metadata) -> row.get("cnt", Integer.class))
                .one()
                .block();
        return count != null && count > 0;
    }

    private static String messageStatus(long id) {
        return db.sql("SELECT status FROM messages WHERE id=:id")
                .bind("id", id)
                .map((row, metadata) -> row.get("status", String.class))
                .one()
                .block();
    }

    private static boolean heartbeatExists(String containerId) {
        Integer count = db.sql("SELECT COUNT(*) AS cnt FROM container_heartbeats WHERE container_id=:containerId")
                .bind("containerId", containerId)
                .map((row, metadata) -> row.get("cnt", Integer.class))
                .one()
                .block();
        return count != null && count > 0;
    }
}
