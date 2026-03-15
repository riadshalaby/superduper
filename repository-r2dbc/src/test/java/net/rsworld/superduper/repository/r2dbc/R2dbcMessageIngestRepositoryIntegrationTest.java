package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Instant;
import java.util.List;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
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
        LiquibaseTestSupport.migrate(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        PostgresqlConnectionFactory cf = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(postgres.getHost())
                .port(postgres.getMappedPort(5432))
                .database(postgres.getDatabaseName())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .build());
        db = DatabaseClient.create(cf);
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

        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");
        repo.upsertReadyMessage("u1", "k1", "v1", occurredAt, null, null).block();
        repo.upsertReadyMessage("u1", "k1", "v2", occurredAt, null, null).block();

        Integer count = db.sql("SELECT COUNT(*) AS c FROM messages WHERE message_id='u1'")
                .map((row, md) -> row.get("c", Integer.class))
                .one()
                .block();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void batchUpsertWorksOnPostgres() {
        R2dbcMessageIngestRepository repo = new R2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();

        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");
        repo.batchUpsertReadyMessages(List.of(
                        new MessageIngestData("default", "u1", "k1", "v1", occurredAt, null, null),
                        new MessageIngestData("default", "u2", "k2", "v2", occurredAt, "corr-2", "order.created"),
                        new MessageIngestData("default", "u1", "k1", "v3", occurredAt, null, null)))
                .block();

        Integer count = db.sql("SELECT COUNT(*) AS c FROM messages WHERE message_id IN ('u1','u2')")
                .map((row, md) -> row.get("c", Integer.class))
                .one()
                .block();
        assertThat(count).isEqualTo(2);
    }
}
