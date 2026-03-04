package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
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

        long first = repo.claimBatch("w1", 10, 5);
        assertThat(first).isEqualTo(2);

        long second = repo.claimBatch("w1", 10, 5);
        assertThat(second).isZero();

        repo.fetchClaimedForWorker("w1")
                .forEach(row -> assertThat(repo.markProcessed(row.id(), "w1")).isTrue());
        long third = repo.claimBatch("w1", 10, 5);
        assertThat(third).isEqualTo(1);
    }

    @Test
    void fetchAndFailureTransitions_work_onMariaDb() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        long claimed = repo.claimBatch("w1", 10, 5);
        assertThat(claimed).isEqualTo(1);

        var rows = repo.fetchClaimedForWorker("w1");
        assertThat(rows).hasSize(1);

        long id = rows.getFirst().id();
        boolean failed = repo.markFailed(id, 1, "w1");
        assertThat(failed).isTrue();
        String failedStatus =
                jdbc.getJdbcTemplate().queryForObject("SELECT status FROM messages WHERE id=" + id, String.class);
        assertThat(failedStatus).isEqualTo("FAILED");
        Integer retry =
                jdbc.getJdbcTemplate().queryForObject("SELECT retry_count FROM messages WHERE id=" + id, Integer.class);
        assertThat(retry).isEqualTo(1);

        long reclaimed = repo.claimBatch("w2", 10, 5);
        assertThat(reclaimed).isEqualTo(1L);

        boolean staleUpdateAccepted = repo.markStopped(id, 2, "w1");
        assertThat(staleUpdateAccepted).isFalse();

        boolean stopByOwner = repo.markStopped(id, 2, "w2");
        assertThat(stopByOwner).isTrue();
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
