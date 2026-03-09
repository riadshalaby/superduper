package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.List;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.mariadb.MariaDBContainer;

class R2dbcCleanupMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static DatabaseClient db;
    static R2dbcWorkerMaintenanceRepository maintenanceRepository;
    static R2dbcWorkerMessageRepository messageRepository;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "mariadb")
                .option(ConnectionFactoryOptions.HOST, mariadb.getHost())
                .option(ConnectionFactoryOptions.PORT, mariadb.getMappedPort(3306))
                .option(ConnectionFactoryOptions.DATABASE, mariadb.getDatabaseName())
                .option(ConnectionFactoryOptions.USER, mariadb.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, mariadb.getPassword())
                .build());
        db = DatabaseClient.create(connectionFactory);
        maintenanceRepository = new R2dbcWorkerMaintenanceRepository(db, R2dbcSqlDialects.from(SqlDialect.MARIADB));
        messageRepository = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(connectionFactory)), SqlDialect.MARIADB);
    }

    @AfterAll
    static void afterAll() {
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @Test
    void deletesExpiredRowsByStatusAndHeartbeatAge() {
        resetData();
        long processedId = insertMessage("processed-old", "k1", "PROCESSED", 20, 0, null);
        long stoppedId = insertMessage("stopped-old", "k2", "STOPPED", 40, 5, null);
        long failedId = insertMessage("failed-old", "k3", "FAILED", 40, 2, null);
        insertHeartbeat("worker-old", 2);
        insertHeartbeat("worker-new", 0);

        assertThat(maintenanceRepository.deleteProcessedOlderThan(14).block()).isEqualTo(1);
        assertThat(maintenanceRepository.deleteStoppedOlderThan(30).block()).isEqualTo(1);
        assertThat(maintenanceRepository.deleteStaleHeartbeats(1).block()).isEqualTo(1);

        assertThat(messageExists(processedId)).isFalse();
        assertThat(messageExists(stoppedId)).isFalse();
        assertThat(messageStatus(failedId)).isEqualTo("FAILED");
        assertThat(heartbeatExists("worker-old")).isFalse();
        assertThat(heartbeatExists("worker-new")).isTrue();
    }

    @Test
    void cleanupPreservesClaimAndRedrivePaths() {
        resetData();
        long readyId = insertMessage("ready-old", "k1", "READY", 45, 0, null);
        long failedId = insertMessage("failed-old", "k2", "FAILED", 45, 1, null);
        long stoppedRecentId = insertMessage("stopped-new", "k3", "STOPPED", 5, 5, null);
        long processingId = insertMessage("processing-old", "k4", "PROCESSING", 45, 1, "worker-old");
        insertMessage("processed-old", "k5", "PROCESSED", 45, 0, null);

        maintenanceRepository.deleteProcessedOlderThan(14).block();
        maintenanceRepository.deleteStoppedOlderThan(30).block();

        assertThat(messageRepository.claimBatch("worker-a", 10, 5).block()).isEqualTo(2L);
        List<ClaimedMessage> claimed = messageRepository
                .fetchClaimedForWorker("worker-a")
                .collectList()
                .block();
        assertThat(claimed).extracting(ClaimedMessage::id).containsExactly(readyId, failedId);
        assertThat(maintenanceRepository.reclaimStaleProcessing(1).block()).isEqualTo(1);
        assertThat(messageRepository.redriveByStatus("STOPPED", 10).block()).isEqualTo(1);
        assertThat(messageStatus(stoppedRecentId)).isEqualTo("READY");
        assertThat(messageStatus(processingId)).isEqualTo("READY");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE container_heartbeats").fetch().rowsUpdated().block();
        db.sql("TRUNCATE TABLE messages").fetch().rowsUpdated().block();
    }

    private static long insertMessage(
            String messageId, String messageKey, String status, int ageDays, int retryCount, String containerId) {
        var statement = db.sql(
                        "INSERT INTO messages(message_id, message_key, content, status, retry_count, container_id, last_updated) "
                                + "VALUES (:messageId, :messageKey, :content, :status, :retryCount, :containerId, "
                                + "TIMESTAMPADD(DAY, -:ageDays, NOW()))")
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
                        + "VALUES (:containerId, TIMESTAMPADD(DAY, -:ageDays, NOW()))")
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
