package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.postgresql.PostgreSQLContainer;

class R2dbcMessageIngestRepositoryIntegrationTest {

    static PostgreSQLContainer postgres;
    static DatabaseClient db;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer("postgres:16-alpine");
        postgres.start();

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(postgres.getHost())
                .port(postgres.getMappedPort(5432))
                .database(postgres.getDatabaseName())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .build());
        db = DatabaseClient.create(cf);

        db.sql("DROP TABLE IF EXISTS messages").fetch().rowsUpdated().block();
        db.sql(
                        "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, last_updated TIMESTAMP DEFAULT NOW(), timestamp TIMESTAMP)")
                .fetch()
                .rowsUpdated()
                .block();
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void upsertWorksOnPostgres() {
        R2dbcMessageIngestRepository repo = new R2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);

        repo.upsertReadyMessage("u1", "k1", "v1").block();
        repo.upsertReadyMessage("u1", "k1", "v2").block();

        Integer count = db.sql("SELECT COUNT(*) AS c FROM messages WHERE uuid='u1'")
                .map((row, md) -> row.get("c", Integer.class))
                .one()
                .block();
        assertThat(count).isEqualTo(1);
    }
}
