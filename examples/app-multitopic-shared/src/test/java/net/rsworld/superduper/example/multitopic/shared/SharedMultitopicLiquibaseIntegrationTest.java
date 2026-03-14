package net.rsworld.superduper.example.multitopic.shared;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

class SharedMultitopicLiquibaseIntegrationTest {

    private static PostgreSQLContainer postgres;
    private static JdbcTemplate jdbc;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        jdbc = new JdbcTemplate(dataSource);

        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void sharedSchemaCreatesMessagesInfraAndClaimIndexes() {
        assertThat(tableExists("messages")).isTrue();
        assertThat(tableExists("container_heartbeats")).isTrue();
        assertThat(tableExists("shedlock")).isTrue();
        assertThat(indexesFor("messages"))
                .contains(
                        "idx_messages_topic_status_key_id",
                        "idx_messages_processing_worker_key_id",
                        "idx_messages_processing_last_updated");
    }

    private static boolean tableExists(String tableName) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
                Integer.class,
                tableName);
        return count != null && count == 1;
    }

    private static List<String> indexesFor(String tableName) {
        return jdbc.queryForList(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = ?",
                String.class,
                tableName);
    }
}
