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
import org.testcontainers.postgresql.PostgreSQLContainer;

class JdbcWorkerClaimExplainIntegrationTest {

    static PostgreSQLContainer postgres;
    static NamedParameterJdbcTemplate jdbc;
    static PostgresJdbcSqlDialect dialect;

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
        dialect = new PostgresJdbcSqlDialect();
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void postgres_claimAndFetchExplainPlansUseIndexes(String scenario) {
        resetData();
        seedScenario(scenario);

        String claimPlan = explain(
                dialect.claimBatchSql(),
                new MapSqlParameterSource()
                        .addValue("cid", "w-claim")
                        .addValue("batch", 200)
                        .addValue("maxRetries", 5)
                        .addValue("topic", "default"));
        String fetchPlan = explain(
                dialect.fetchClaimedForWorkerSql(),
                new MapSqlParameterSource().addValue("cid", "w-fetch").addValue("topic", "default"));

        assertThat(claimPlan)
                .containsAnyOf("idx_messages_topic_status_key_id", "idx_messages_processing_worker_key_id")
                .doesNotContain("Seq Scan");
        assertThat(fetchPlan).contains("idx_messages_processing_worker_key_id").doesNotContain("Seq Scan");
    }

    static Stream<String> scenarios() {
        return Stream.of("uniform", "hot-key", "mixed-status", "high-retry-failed");
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages RESTART IDENTITY");
    }

    private static void seedScenario(String scenario) {
        String insertSql =
                switch (scenario) {
                    case "uniform" ->
                        "INSERT INTO messages(topic,message_id,message_key,content,status,retry_count,last_updated) "
                                + "SELECT CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END, "
                                + "LPAD(g::text, 36, '0'), 'k' || (g % 100), 'v', 'READY', 0, "
                                + "NOW() - ((g % 1000) || ' seconds')::interval FROM generate_series(1, 10000) g";
                    case "hot-key" ->
                        "INSERT INTO messages(topic,message_id,message_key,content,status,retry_count,last_updated) "
                                + "SELECT CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END, LPAD(g::text, 36, '0'), "
                                + "CASE WHEN g <= 5000 THEN 'hot-key' ELSE 'k' || ((g - 5000) % 99) END, "
                                + "'v', 'READY', 0, NOW() - ((g % 1000) || ' seconds')::interval "
                                + "FROM generate_series(1, 10000) g";
                    case "mixed-status" ->
                        "INSERT INTO messages(topic,message_id,message_key,content,status,retry_count,container_id,last_updated) "
                                + "SELECT CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END, "
                                + "LPAD(g::text, 36, '0'), 'k' || (g % 100), 'v', "
                                + "CASE WHEN g % 5 = 0 THEN 'FAILED' "
                                + "WHEN g % 7 = 0 THEN 'PROCESSING' "
                                + "WHEN g % 11 = 0 THEN 'STOPPED' "
                                + "WHEN g % 13 = 0 THEN 'PROCESSED' "
                                + "ELSE 'READY' END, "
                                + "CASE WHEN g % 5 = 0 THEN 4 ELSE 0 END, "
                                + "CASE WHEN g % 7 = 0 THEN 'w-other' ELSE NULL END, "
                                + "NOW() - ((g % 1000) || ' seconds')::interval "
                                + "FROM generate_series(1, 10000) g";
                    case "high-retry-failed" ->
                        "INSERT INTO messages(topic,message_id,message_key,content,status,retry_count,last_updated) "
                                + "SELECT CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END, "
                                + "LPAD(g::text, 36, '0'), 'k' || (g % 100), 'v', "
                                + "CASE WHEN g % 5 = 0 THEN 'FAILED' ELSE 'READY' END, "
                                + "CASE WHEN g % 5 = 0 THEN 4 + (g % 2) ELSE 0 END, "
                                + "NOW() - ((g % 1000) || ' seconds')::interval "
                                + "FROM generate_series(1, 10000) g";
                    default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
                };
        jdbc.getJdbcTemplate().execute(insertSql);
        jdbc.getJdbcTemplate()
                .execute("UPDATE messages SET status='PROCESSING', container_id='w-fetch' "
                        + "WHERE id % 97 = 0 AND topic = 'default'");
    }

    private static String explain(String sql, MapSqlParameterSource params) {
        List<String> plan = jdbc.queryForList("EXPLAIN " + sql, params, String.class);
        return String.join(System.lineSeparator(), plan);
    }
}
