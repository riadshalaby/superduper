package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;

class JdbcWorkerMessageRepositoryMariaDbIntegrationTest {

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

        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute(
                "CREATE TABLE messages (id BIGINT AUTO_INCREMENT PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, `key` VARCHAR(255) NOT NULL, content TEXT, status VARCHAR(32) NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        jt.execute("CREATE INDEX idx_messages_key_id ON messages(`key`, id)");
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

        List<Long> first = repo.claimBatch("w1", 10, 5);
        assertThat(first).hasSize(2);

        List<Long> second = repo.claimBatch("w1", 10, 5);
        assertThat(second).isEmpty();

        first.forEach(repo::markProcessed);
        List<Long> third = repo.claimBatch("w1", 10, 5);
        assertThat(third).hasSize(1);
    }

    @Test
    void fetchAndRetryTransitions_work_onMariaDb() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        List<Long> claimed = repo.claimBatch("w1", 10, 5);
        assertThat(claimed).hasSize(1);

        var rows = repo.fetchClaimedByIds(claimed);
        assertThat(rows).hasSize(1);

        long id = rows.getFirst().id();
        repo.markReadyForRetry(id, 1);
        Integer retry =
                jdbc.getJdbcTemplate().queryForObject("SELECT retry_count FROM messages WHERE id=" + id, Integer.class);
        assertThat(retry).isEqualTo(1);

        repo.markStopped(id, 2);
        String status =
                jdbc.getJdbcTemplate().queryForObject("SELECT status FROM messages WHERE id=" + id, String.class);
        assertThat(status).isEqualTo("STOPPED");
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages");
    }

    private static void insert(String uuid, String key, String content, String status) {
        jdbc.getJdbcTemplate()
                .update("INSERT INTO messages(uuid,`key`,content,status) VALUES (?,?,?,?)", uuid, key, content, status);
    }
}
