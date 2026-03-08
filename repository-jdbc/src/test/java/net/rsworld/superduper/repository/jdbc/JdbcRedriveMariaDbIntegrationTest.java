package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;

class JdbcRedriveMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static NamedParameterJdbcTemplate jdbc;
    static JdbcWorkerMessageRepository repo;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUrl(mariadb.getJdbcUrl());
        ds.setUsername(mariadb.getUsername());
        ds.setPassword(mariadb.getPassword());

        jdbc = new NamedParameterJdbcTemplate(ds);
        repo = new JdbcWorkerMessageRepository(jdbc, SqlDialect.MARIADB);
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @Test
    void findByStatus_returnsFailedRowsOrderedAndLimited() {
        resetData();
        long failed1 = insert("f-1", "k1", "v1", "FAILED", 2, null);
        insert("r-1", "k1", "v2", "READY", 0, null);
        long failed2 = insert("f-2", "k2", "v3", "FAILED", 1, null);
        insert("s-1", "k3", "v4", "STOPPED", 5, null);

        List<ClaimedMessage> rows = repo.findByStatus("FAILED", 2);

        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(failed1, failed2);
        assertThat(rows).extracting(ClaimedMessage::retryCount).containsExactly(2, 1);
    }

    @Test
    void findByStatus_returnsStoppedRowsOnly() {
        resetData();
        long stopped1 = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        insert("f-1", "k2", "v2", "FAILED", 1, null);
        long stopped2 = insert("s-2", "k3", "v3", "STOPPED", 6, null);

        List<ClaimedMessage> rows = repo.findByStatus("STOPPED", 10);

        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(stopped1, stopped2);
    }

    @Test
    void redriveById_transitionsFailedRowToReady() {
        resetData();
        long id = insert("f-1", "k1", "v1", "FAILED", 3, "worker-a");

        assertThat(repo.redriveById(id)).isEqualTo(1);
        assertThat(messageRow(id))
                .containsEntry("status", "READY")
                .containsEntry("retry_count", 0)
                .containsEntry("container_id", null);
    }

    @Test
    void redriveById_transitionsStoppedRowToReady() {
        resetData();
        long id = insert("s-1", "k1", "v1", "STOPPED", 5, "worker-a");

        assertThat(repo.redriveById(id)).isEqualTo(1);
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

        assertThat(repo.redriveById(readyId)).isZero();
        assertThat(repo.redriveById(processingId)).isZero();
    }

    @Test
    void redriveByStatus_redrivesUpToLimit() {
        resetData();
        long id1 = insert("f-1", "k1", "v1", "FAILED", 2, null);
        long id2 = insert("f-2", "k2", "v2", "FAILED", 3, null);
        long id3 = insert("f-3", "k3", "v3", "FAILED", 4, null);

        assertThat(repo.redriveByStatus("FAILED", 2)).isEqualTo(2);

        assertThat(messageRow(id1)).containsEntry("status", "READY").containsEntry("retry_count", 0);
        assertThat(messageRow(id2)).containsEntry("status", "READY").containsEntry("retry_count", 0);
        assertThat(messageRow(id3)).containsEntry("status", "FAILED").containsEntry("retry_count", 4);
    }

    @Test
    void redrivenStoppedMessageWithReadySibling_isClaimedInIdOrder() {
        resetData();
        long stoppedId = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        long readyId = insert("r-1", "k1", "v2", "READY", 0, null);

        assertThat(repo.redriveById(stoppedId)).isEqualTo(1);
        assertThat(repo.claimBatch("w1", 10, 5)).isEqualTo(2);

        List<ClaimedMessage> rows = repo.fetchClaimedForWorker("w1");
        assertThat(rows).extracting(ClaimedMessage::id).containsExactly(stoppedId, readyId);
    }

    @Test
    void redrivenStoppedMessageWithProcessingSibling_waitsUntilSiblingCompletes() {
        resetData();
        long stoppedId = insert("s-1", "k1", "v1", "STOPPED", 5, null);
        long processingId = insert("p-1", "k1", "v2", "PROCESSING", 1, "worker-other");

        assertThat(repo.redriveById(stoppedId)).isEqualTo(1);
        assertThat(repo.claimBatch("w1", 10, 5)).isZero();

        jdbc.getJdbcTemplate()
                .update("UPDATE messages SET status='PROCESSED', processed_at=NOW() WHERE id=?", processingId);

        assertThat(repo.claimBatch("w1", 10, 5)).isEqualTo(1);
        assertThat(repo.fetchClaimedForWorker("w1"))
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

        assertThat(repo.countByStatus())
                .containsEntry("READY", 1L)
                .containsEntry("FAILED", 1L)
                .containsEntry("STOPPED", 1L)
                .containsEntry("PROCESSING", 1L);
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages");
    }

    private static long insert(
            String messageId, String messageKey, String content, String status, int retryCount, String containerId) {
        jdbc.getJdbcTemplate()
                .update(
                        "INSERT INTO messages(message_id,message_key,content,status,retry_count,container_id,last_updated) "
                                + "VALUES (?,?,?,?,?,?,NOW())",
                        messageId,
                        messageKey,
                        content,
                        status,
                        retryCount,
                        containerId);
        return jdbc.getJdbcTemplate().queryForObject("SELECT MAX(id) FROM messages", Long.class);
    }

    private static Map<String, Object> messageRow(long id) {
        return jdbc.getJdbcTemplate()
                .queryForMap("SELECT status, retry_count, container_id FROM messages WHERE id=?", id);
    }
}
