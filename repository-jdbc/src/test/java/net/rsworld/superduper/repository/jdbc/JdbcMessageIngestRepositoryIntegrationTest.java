package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

class JdbcMessageIngestRepositoryIntegrationTest {

    static PostgreSQLContainer postgres;
    static MariaDBContainer mariadb;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @Test
    void upsertWorksOnPostgres() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUsername(postgres.getUsername());
        ds.setPassword(postgres.getPassword());

        JdbcTemplate jdbc = new JdbcTemplate(ds);

        JdbcMessageIngestRepository repo = new JdbcMessageIngestRepository(
                new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(ds), SqlDialect.POSTGRES);
        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");
        repo.upsertReadyMessage("u1", "k1", "v1", occurredAt);
        repo.upsertReadyMessage("u1", "k1", "v2", occurredAt);

        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM messages WHERE uuid='u1'", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void upsertWorksOnMariaDb() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUrl(mariadb.getJdbcUrl());
        ds.setUsername(mariadb.getUsername());
        ds.setPassword(mariadb.getPassword());

        JdbcTemplate jdbc = new JdbcTemplate(ds);

        JdbcMessageIngestRepository repo = new JdbcMessageIngestRepository(
                new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(ds), SqlDialect.MARIADB);
        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");
        repo.upsertReadyMessage("u1", "k1", "v1", occurredAt);
        repo.upsertReadyMessage("u1", "k1", "v2", occurredAt);

        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM messages WHERE uuid='u1'", Integer.class);
        assertThat(count).isEqualTo(1);
    }
}
