package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.mariadb.MariaDBContainer;

class R2dbcWorkerClaimExplainMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static DatabaseClient db;
    static MariaDbR2dbcSqlDialect dialect;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());

        ConnectionFactory cf = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "mariadb")
                .option(ConnectionFactoryOptions.HOST, mariadb.getHost())
                .option(ConnectionFactoryOptions.PORT, mariadb.getMappedPort(3306))
                .option(ConnectionFactoryOptions.DATABASE, mariadb.getDatabaseName())
                .option(ConnectionFactoryOptions.USER, mariadb.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, mariadb.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        dialect = new MariaDbR2dbcSqlDialect();
        new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.MARIADB);
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
                dialect.claimBatchSql(), Map.of("cid", "w-claim", "batch", 200, "maxRetries", 5, "topic", "default"));
        List<ExplainRow> fetchPlan =
                explain(dialect.fetchClaimedForWorkerSql(), Map.of("cid", "w-fetch", "topic", "default"));

        assertThat(planKeys(claimPlan))
                .containsAnyOf(
                        "idx_messages_topic_status_key_id", "idx_messages_processing_worker_status_container_key_id");
        assertThat(planRow(claimPlan, "m1").type()).isNotEqualTo("ALL");
        assertThat(planKeys(fetchPlan))
                .containsAnyOf(
                        "idx_messages_processing_worker_status_container_key_id", "idx_messages_topic_status_key_id");
        assertThat(planRow(fetchPlan, "messages").type()).isNotEqualTo("ALL");
    }

    static Stream<String> scenarios() {
        return Stream.of("uniform", "hot-key", "mixed-status", "high-retry-failed");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE messages").fetch().rowsUpdated().block();
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
        db.sql(insertSql).fetch().rowsUpdated().block();
        db.sql("UPDATE messages SET status='PROCESSING', container_id='w-fetch' "
                        + "WHERE id % 97 = 0 AND topic = 'default'")
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static String baseInsertSql(
            String keyExpression, String statusExpression, String retryExpression, String containerExpression) {
        return "INSERT INTO messages(topic,message_id,message_key,content,status,retry_count,container_id,last_updated) "
                + "SELECT CASE WHEN MOD(t.n, 2) = 0 THEN 'default' ELSE 'topic-b' END, UUID(), "
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

    private static List<ExplainRow> explain(String sql, Map<String, Object> binds) {
        var spec = db.sql("EXPLAIN " + sql);
        for (Map.Entry<String, Object> bind : binds.entrySet()) {
            spec = spec.bind(bind.getKey(), bind.getValue());
        }
        return spec.map((row, metadata) -> new ExplainRow(
                        row.get("table", String.class), row.get("key", String.class), row.get("type", String.class)))
                .all()
                .collectList()
                .block();
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
