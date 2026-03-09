package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;

class JdbcCleanupMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static NamedParameterJdbcTemplate jdbc;
    static JdbcWorkerMaintenanceRepository maintenanceRepository;
    static JdbcWorkerMessageRepository messageRepository;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.mariadb.jdbc.Driver");
        dataSource.setUrl(mariadb.getJdbcUrl());
        dataSource.setUsername(mariadb.getUsername());
        dataSource.setPassword(mariadb.getPassword());

        jdbc = new NamedParameterJdbcTemplate(dataSource);
        maintenanceRepository = new JdbcWorkerMaintenanceRepository(jdbc, JdbcSqlDialects.from(SqlDialect.MARIADB));
        messageRepository = new JdbcWorkerMessageRepository(jdbc, SqlDialect.MARIADB);
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
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

        assertThat(maintenanceRepository.deleteProcessedOlderThan(14)).isEqualTo(1);
        assertThat(maintenanceRepository.deleteStoppedOlderThan(30)).isEqualTo(1);
        assertThat(maintenanceRepository.deleteStaleHeartbeats(1)).isEqualTo(1);

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

        maintenanceRepository.deleteProcessedOlderThan(14);
        maintenanceRepository.deleteStoppedOlderThan(30);

        assertThat(messageRepository.claimBatch("worker-a", 10, 5)).isEqualTo(2);
        List<ClaimedMessage> claimed = messageRepository.fetchClaimedForWorker("worker-a");
        assertThat(claimed).extracting(ClaimedMessage::id).containsExactly(readyId, failedId);
        assertThat(maintenanceRepository.reclaimStaleProcessing(1)).isEqualTo(1);
        assertThat(messageRepository.redriveByStatus("STOPPED", 10)).isEqualTo(1);
        assertThat(messageStatus(stoppedRecentId)).isEqualTo("READY");
        assertThat(messageStatus(processingId)).isEqualTo("READY");
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE container_heartbeats");
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages");
    }

    private static long insertMessage(
            String messageId, String messageKey, String status, int ageDays, int retryCount, String containerId) {
        jdbc.update(
                "INSERT INTO messages(message_id, message_key, content, status, retry_count, container_id, last_updated) "
                        + "VALUES (:messageId, :messageKey, :content, :status, :retryCount, :containerId, "
                        + "TIMESTAMPADD(DAY, -:ageDays, NOW()))",
                new MapSqlParameterSource()
                        .addValue("messageId", messageId)
                        .addValue("messageKey", messageKey)
                        .addValue("content", "payload-" + messageId)
                        .addValue("status", status)
                        .addValue("retryCount", retryCount)
                        .addValue("containerId", containerId)
                        .addValue("ageDays", ageDays));
        return jdbc.getJdbcTemplate().queryForObject("SELECT MAX(id) FROM messages", Long.class);
    }

    private static void insertHeartbeat(String containerId, int ageDays) {
        jdbc.update(
                "INSERT INTO container_heartbeats(container_id, last_heartbeat) "
                        + "VALUES (:containerId, TIMESTAMPADD(DAY, -:ageDays, NOW()))",
                new MapSqlParameterSource().addValue("containerId", containerId).addValue("ageDays", ageDays));
    }

    private static boolean messageExists(long id) {
        Integer count =
                jdbc.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM messages WHERE id=" + id, Integer.class);
        return count != null && count > 0;
    }

    private static String messageStatus(long id) {
        return jdbc.getJdbcTemplate().queryForObject("SELECT status FROM messages WHERE id=" + id, String.class);
    }

    private static boolean heartbeatExists(String containerId) {
        Integer count = jdbc.getJdbcTemplate()
                .queryForObject(
                        "SELECT COUNT(*) FROM container_heartbeats WHERE container_id=?", Integer.class, containerId);
        return count != null && count > 0;
    }
}
