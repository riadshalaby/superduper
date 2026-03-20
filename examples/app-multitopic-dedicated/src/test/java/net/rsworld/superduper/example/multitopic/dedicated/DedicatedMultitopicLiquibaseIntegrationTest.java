package net.rsworld.superduper.example.multitopic.dedicated;

import static org.assertj.core.api.Assertions.assertThat;

import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

class DedicatedMultitopicLiquibaseIntegrationTest {

    private static final String DEDICATED_CHANGELOG = "db/changelog/multitopic-dedicated/db.changelog-dedicated.yaml";

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

        LiquibaseTestSupport.migrate(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), DEDICATED_CHANGELOG);
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void dedicatedSchemaCreatesOnlyInfraAndPerTopicTables() {
        assertThat(tableExists("container_heartbeats")).isTrue();
        assertThat(tableExists("shedlock")).isTrue();
        assertThat(tableExists("orders_messages")).isTrue();
        assertThat(tableExists("invoices_messages")).isTrue();
        assertThat(tableExists("outbox_notifications")).isTrue();
        assertThat(tableExists("messages")).isFalse();
    }

    private static boolean tableExists(String tableName) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
                Integer.class,
                tableName);
        return count != null && count == 1;
    }
}
