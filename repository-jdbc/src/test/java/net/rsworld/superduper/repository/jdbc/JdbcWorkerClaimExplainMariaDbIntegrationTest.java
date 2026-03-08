package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;

class JdbcWorkerClaimExplainMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static NamedParameterJdbcTemplate jdbc;
    static MariaDbJdbcSqlDialect dialect;

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
        dialect = new MariaDbJdbcSqlDialect();
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void mariadb_claimAndFetchExplainPlansUseIndexes(String scenario) {
        resetData();
        seedScenario(scenario);

        List<ExplainRow> claimPlan = explain(
                dialect.claimBatchSql(),
                new MapSqlParameterSource()
                        .addValue("cid", "w-claim")
                        .addValue("batch", 200)
                        .addValue("maxRetries", 5));
        List<ExplainRow> fetchPlan =
                explain(dialect.fetchClaimedForWorkerSql(), new MapSqlParameterSource().addValue("cid", "w-fetch"));

        assertThat(planKeys(claimPlan))
                .containsAnyOf(
                        "idx_messages_claim_status_id_key",
                        "idx_messages_claim_failed_status_retry_id_key",
                        "idx_messages_processing_exists_key_status");
        assertThat(planRow(claimPlan, "m1").type()).isNotEqualTo("ALL");
        assertThat(planKeys(fetchPlan)).contains("idx_messages_processing_worker_status_container_key_id");
        assertThat(planRow(fetchPlan, "messages").type()).isNotEqualTo("ALL");
    }

    static Stream<String> scenarios() {
        return Stream.of("uniform", "hot-key", "mixed-status", "high-retry-failed");
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages");
    }

    private static void seedScenario(String scenario) {
        String insertSql =
                switch (scenario) {
                    case "uniform" -> baseInsertSql("CONCAT('k', MOD(t.n, 100))", "'READY'", "0", "NULL");
                    case "hot-key" ->
                        baseInsertSql(
                                "CASE WHEN t.n <= 5000 THEN 'hot-key' ELSE CONCAT('k', MOD(t.n, 99)) END",
                                "'READY'",
                                "0",
                                "NULL");
                    case "mixed-status" ->
                        baseInsertSql(
                                "CONCAT('k', MOD(t.n, 100))",
                                "CASE WHEN MOD(t.n, 5) = 0 THEN 'FAILED' "
                                        + "WHEN MOD(t.n, 7) = 0 THEN 'PROCESSING' "
                                        + "WHEN MOD(t.n, 11) = 0 THEN 'STOPPED' "
                                        + "WHEN MOD(t.n, 13) = 0 THEN 'PROCESSED' "
                                        + "ELSE 'READY' END",
                                "CASE WHEN MOD(t.n, 5) = 0 THEN 4 ELSE 0 END",
                                "CASE WHEN MOD(t.n, 7) = 0 THEN 'w-other' ELSE NULL END");
                    case "high-retry-failed" ->
                        baseInsertSql(
                                "CONCAT('k', MOD(t.n, 100))",
                                "CASE WHEN MOD(t.n, 5) = 0 THEN 'FAILED' ELSE 'READY' END",
                                "CASE WHEN MOD(t.n, 5) = 0 THEN 4 + MOD(t.n, 2) ELSE 0 END",
                                "NULL");
                    default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
                };
        jdbc.getJdbcTemplate().execute(insertSql);
        jdbc.getJdbcTemplate()
                .execute("UPDATE messages SET status='PROCESSING', container_id='w-fetch' WHERE id % 97 = 0");
    }

    private static String baseInsertSql(
            String keyExpression, String statusExpression, String retryExpression, String containerExpression) {
        return "INSERT INTO messages(message_id,message_key,content,status,retry_count,container_id,last_updated) "
                + "SELECT UUID(), "
                + keyExpression
                + ", 'v', "
                + statusExpression
                + ", "
                + retryExpression
                + ", "
                + containerExpression
                + ", NOW() - INTERVAL (MOD(t.n, 1000)) SECOND "
                + "FROM ("
                + "SELECT a.d + b.d * 10 + c.d * 100 + d.d * 1000 + 1 AS n "
                + "FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a "
                + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b "
                + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c "
                + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d"
                + ") t";
    }

    private static List<ExplainRow> explain(String sql, MapSqlParameterSource params) {
        return jdbc.queryForList("EXPLAIN " + sql, params).stream()
                .map(row -> new ExplainRow(
                        String.valueOf(row.get("table")),
                        String.valueOf(row.get("key")),
                        String.valueOf(row.get("type"))))
                .toList();
    }

    private static List<String> planKeys(List<ExplainRow> plan) {
        return plan.stream().map(ExplainRow::key).toList();
    }

    private static ExplainRow planRow(List<ExplainRow> plan, String table) {
        return plan.stream()
                .filter(row -> table.equals(row.table()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No EXPLAIN row found for table " + table + ": " + plan));
    }

    private record ExplainRow(String table, String key, String type) {}
}
