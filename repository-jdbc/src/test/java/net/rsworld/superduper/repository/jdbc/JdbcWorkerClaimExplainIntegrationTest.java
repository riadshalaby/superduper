package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

class JdbcWorkerClaimExplainIntegrationTest {

    @Test
    void postgres_claimBatchSingleStatement_explain() {
        try (PostgreSQLContainer pg = new PostgreSQLContainer("postgres:16-alpine")) {
            pg.start();
            DriverManagerDataSource ds = new DriverManagerDataSource();
            ds.setDriverClassName("org.postgresql.Driver");
            ds.setUrl(pg.getJdbcUrl());
            ds.setUsername(pg.getUsername());
            ds.setPassword(pg.getPassword());
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(ds);
            LiquibaseTestSupport.migrate(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
            jdbc.getJdbcTemplate()
                    .execute("INSERT INTO messages(message_id,message_key,content,status,retry_count,last_updated) "
                            + "SELECT LPAD(g::text, 36, '0'), 'k' || (g % 500), 'v', "
                            + "CASE WHEN g % 13 = 0 THEN 'FAILED' ELSE 'READY' END, "
                            + "CASE WHEN g % 13 = 0 THEN (g % 4) ELSE 0 END, "
                            + "NOW() - ((g % 1000) || ' seconds')::interval "
                            + "FROM generate_series(1, 50000) g");
            jdbc.getJdbcTemplate()
                    .execute("UPDATE messages SET status='PROCESSING', container_id='w-other' WHERE id % 197 = 0");

            String explainSql = "EXPLAIN " + new PostgresJdbcSqlDialect().claimBatchSql();
            List<String> plan = jdbc.queryForList(
                    explainSql,
                    new MapSqlParameterSource()
                            .addValue("cid", "w1")
                            .addValue("batch", 200)
                            .addValue("maxRetries", 5),
                    String.class);
            String planText = String.join(System.lineSeparator(), plan);

            System.out.println("POSTGRES CLAIM PLAN");
            System.out.println(planText);

            assertThat(planText).contains("Update on");
            assertThat(planText).contains("CTE candidate");
            assertThat(planText).contains("LockRows");
            assertThat(planText).contains("messages");
        }
    }

    @Test
    void mariadb_claimBatchSingleStatement_explain() {
        try (MariaDBContainer mariadb = new MariaDBContainer("mariadb:11.4")) {
            mariadb.start();
            DriverManagerDataSource ds = new DriverManagerDataSource();
            ds.setDriverClassName("org.mariadb.jdbc.Driver");
            ds.setUrl(mariadb.getJdbcUrl());
            ds.setUsername(mariadb.getUsername());
            ds.setPassword(mariadb.getPassword());
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(ds);
            LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
            jdbc.getJdbcTemplate()
                    .execute("INSERT INTO messages(message_id,message_key,content,status,retry_count,last_updated) "
                            + "SELECT UUID(), CONCAT('k', MOD(t.n, 500)), 'v', "
                            + "IF(MOD(t.n, 13) = 0, 'FAILED', 'READY'), "
                            + "IF(MOD(t.n, 13) = 0, MOD(t.n, 4), 0), "
                            + "NOW() - INTERVAL (MOD(t.n, 1000)) SECOND "
                            + "FROM ("
                            + "SELECT a.d + b.d * 10 + c.d * 100 + d.d * 1000 + e.d * 10000 + 1 AS n "
                            + "FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                            + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a "
                            + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                            + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b "
                            + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                            + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c "
                            + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
                            + "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d "
                            + "CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) e"
                            + ") t");
            jdbc.getJdbcTemplate()
                    .execute("UPDATE messages SET status='PROCESSING', container_id='w-other' WHERE id % 197 = 0");

            String explainSql = "EXPLAIN FORMAT=JSON " + new MariaDbJdbcSqlDialect().claimBatchSql();
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("cid", "w1")
                    .addValue("batch", 200)
                    .addValue("maxRetries", 5);

            jdbc.getJdbcTemplate().execute("DROP INDEX IF EXISTS idx_messages_claim_status_id_key ON messages");
            jdbc.getJdbcTemplate()
                    .execute("DROP INDEX IF EXISTS idx_messages_claim_failed_status_retry_id_key ON messages");
            jdbc.getJdbcTemplate()
                    .execute("DROP INDEX IF EXISTS idx_messages_processing_exists_key_status ON messages");
            jdbc.getJdbcTemplate()
                    .execute("DROP INDEX IF EXISTS idx_messages_processing_worker_status_container_key_id ON messages");
            jdbc.getJdbcTemplate()
                    .execute("DROP INDEX IF EXISTS idx_messages_processing_stale_status_last_updated ON messages");

            List<String> baselinePlan = jdbc.queryForList(explainSql, params, String.class);
            String baselinePlanText = String.join(System.lineSeparator(), baselinePlan);

            System.out.println("MARIADB CLAIM PLAN (baseline)");
            System.out.println(baselinePlanText);

            jdbc.getJdbcTemplate()
                    .execute("CREATE INDEX idx_messages_claim_status_id_key ON messages(status, id, message_key)");
            jdbc.getJdbcTemplate()
                    .execute("CREATE INDEX idx_messages_claim_failed_status_retry_id_key "
                            + "ON messages(status, retry_count, id, message_key)");
            jdbc.getJdbcTemplate()
                    .execute("CREATE INDEX idx_messages_processing_exists_key_status ON messages(message_key, status)");
            jdbc.getJdbcTemplate()
                    .execute("CREATE INDEX idx_messages_processing_worker_status_container_key_id "
                            + "ON messages(status, container_id, message_key, id)");
            jdbc.getJdbcTemplate()
                    .execute(
                            "CREATE INDEX idx_messages_processing_stale_status_last_updated ON messages(status, last_updated)");

            List<String> tunedPlan = jdbc.queryForList(explainSql, params, String.class);
            String tunedPlanText = String.join(System.lineSeparator(), tunedPlan);

            System.out.println("MARIADB CLAIM PLAN (with tuned indexes)");
            System.out.println(tunedPlanText);

            assertThat(baselinePlanText).contains("messages");
            assertThat(baselinePlanText).contains("\"table_name\": \"m1\"");
            assertThat(tunedPlanText).contains("messages");
            assertThat(tunedPlanText)
                    .containsAnyOf(
                            "idx_messages_claim_status_id_key",
                            "idx_messages_claim_failed_status_retry_id_key",
                            "idx_messages_processing_exists_key_status");
        }
    }
}
