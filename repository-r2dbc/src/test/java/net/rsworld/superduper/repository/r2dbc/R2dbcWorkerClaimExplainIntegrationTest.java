package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.util.stream.Stream;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;

class R2dbcWorkerClaimExplainIntegrationTest {

    static PostgreSQLContainer postgres;
    static DatabaseClient db;
    static PostgresR2dbcSqlDialect dialect;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(postgres.getHost())
                .port(postgres.getMappedPort(5432))
                .database(postgres.getDatabaseName())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .build());
        db = DatabaseClient.create(cf);
        dialect = new PostgresR2dbcSqlDialect();
        new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.POSTGRES);
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

        String claimPlan = explain(dialect.claimBatchSql(), "w-claim", 200, 5, "default");
        String fetchPlan = explainFetch(dialect.fetchClaimedForWorkerSql(), "w-fetch", "default");

        assertThat(claimPlan)
                .containsAnyOf("idx_messages_topic_status_key_id", "idx_messages_processing_worker_key_id")
                .doesNotContain("Seq Scan");
        assertThat(fetchPlan).contains("idx_messages_processing_worker_key_id").doesNotContain("Seq Scan");
    }

    static Stream<String> scenarios() {
        return Stream.of("uniform", "hot-key", "mixed-status", "high-retry-failed");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
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
                                + "SELECT CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END, "
                                + "LPAD(g::text, 36, '0'), "
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
        db.sql(insertSql).fetch().rowsUpdated().block();
        db.sql("UPDATE messages SET status='PROCESSING', container_id='w-fetch' "
                        + "WHERE id % 97 = 0 AND topic = 'default'")
                .fetch()
                .rowsUpdated()
                .block();
    }

    private static String explain(String sql, String cid, int batch, int maxRetries, String topic) {
        return String.join(
                System.lineSeparator(),
                db.sql("EXPLAIN " + sql)
                        .bind("cid", cid)
                        .bind("batch", batch)
                        .bind("maxRetries", maxRetries)
                        .bind("topic", topic)
                        .map((row, metadata) -> row.get(0, String.class))
                        .all()
                        .collectList()
                        .block());
    }

    private static String explainFetch(String sql, String cid, String topic) {
        return String.join(
                System.lineSeparator(),
                db.sql("EXPLAIN " + sql)
                        .bind("cid", cid)
                        .bind("topic", topic)
                        .map((row, metadata) -> row.get(0, String.class))
                        .all()
                        .collectList()
                        .block());
    }
}
