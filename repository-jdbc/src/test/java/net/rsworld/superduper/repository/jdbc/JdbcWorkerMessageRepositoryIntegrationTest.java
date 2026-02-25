package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

class JdbcWorkerMessageRepositoryIntegrationTest {

    static PostgreSQLContainer postgres;
    static NamedParameterJdbcTemplate jdbc;
    static JdbcWorkerMessageRepository repo;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUsername(postgres.getUsername());
        ds.setPassword(postgres.getPassword());

        jdbc = new NamedParameterJdbcTemplate(ds);
        repo = new JdbcWorkerMessageRepository(jdbc, SqlDialect.POSTGRES);

        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute(
                "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())");
        jt.execute("CREATE INDEX idx_messages_key_id ON messages(key, id)");
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

        long first = repo.claimBatch("w1", 10, 5);
        assertThat(first).isEqualTo(2);

        long second = repo.claimBatch("w1", 10, 5);
        assertThat(second).isZero();

        var firstRows = repo.fetchClaimedForWorker("w1");
        assertThat(firstRows).hasSize(2);
        List<Long> firstIds = new ArrayList<>();
        for (var row : firstRows) {
            firstIds.add(row.id());
            repo.markProcessed(row.id());
        }
        Integer processedAtCount = jdbc.getJdbcTemplate()
                .queryForObject(
                        "SELECT COUNT(*) FROM messages WHERE id IN (" + firstIds.get(0) + "," + firstIds.get(1) + ") "
                                + "AND processed_at IS NOT NULL",
                        Integer.class);
        assertThat(processedAtCount).isEqualTo(2);
        long third = repo.claimBatch("w1", 10, 5);
        assertThat(third).isEqualTo(1);
    }

    @Test
    void fetchAndRetryTransitions_work() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        long claimed = repo.claimBatch("w1", 10, 5);
        assertThat(claimed).isEqualTo(1);

        var rows = repo.fetchClaimedForWorker("w1");
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
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages RESTART IDENTITY");
    }

    private static void insert(String uuid, String key, String content, String status) {
        jdbc.getJdbcTemplate()
                .update("INSERT INTO messages(uuid,key,content,status) VALUES (?,?,?,?)", uuid, key, content, status);
    }
}
