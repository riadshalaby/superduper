package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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

        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();
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
        jdbc.execute("DROP TABLE IF EXISTS messages");
        jdbc.execute(
                "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())");

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
        jdbc.execute("DROP TABLE IF EXISTS messages");
        jdbc.execute(
                "CREATE TABLE messages (id BIGINT AUTO_INCREMENT PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, `key` VARCHAR(255) NOT NULL, content TEXT, status VARCHAR(32) NOT NULL, retry_count INT DEFAULT 0, occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");

        JdbcMessageIngestRepository repo = new JdbcMessageIngestRepository(
                new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(ds), SqlDialect.MARIADB);
        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");
        repo.upsertReadyMessage("u1", "k1", "v1", occurredAt);
        repo.upsertReadyMessage("u1", "k1", "v2", occurredAt);

        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM messages WHERE uuid='u1'", Integer.class);
        assertThat(count).isEqualTo(1);
    }
}
