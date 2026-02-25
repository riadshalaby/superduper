package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;

class R2dbcWorkerMessageRepositoryIntegrationTest {

    static PostgreSQLContainer postgres;
    static DatabaseClient db;
    static R2dbcWorkerMessageRepository repo;

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
        repo = new R2dbcWorkerMessageRepository(
                db, TransactionalOperator.create(new R2dbcTransactionManager(cf)), SqlDialect.POSTGRES);

        Flux.concat(
                        db.sql(
                                        "CREATE TABLE messages (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, key VARCHAR(255) NOT NULL, content TEXT, status TEXT NOT NULL, retry_count INT DEFAULT 0, container_id VARCHAR(255), occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, processed_at TIMESTAMP NULL, last_updated TIMESTAMP DEFAULT NOW())")
                                .fetch()
                                .rowsUpdated(),
                        db.sql("CREATE INDEX idx_messages_key_id ON messages(key, id)")
                                .fetch()
                                .rowsUpdated())
                .then()
                .block();
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void claimBatch_respectsPerKeyOrdering() {
        resetData();
        insert("u1", "k1", "v1", "READY");
        insert("u2", "k1", "v2", "READY");
        insert("u3", "k2", "v3", "READY");

        Long first = repo.claimBatch("w1", 10, 5).block();
        assertThat(first).isEqualTo(2L);

        Long second = repo.claimBatch("w1", 10, 5).block();
        assertThat(second).isZero();

        var firstRows = repo.fetchClaimedForWorker("w1").collectList().block();
        assertThat(firstRows).isNotNull();
        assertThat(firstRows).hasSize(2);
        firstRows.forEach(row -> repo.markProcessed(row.id()).block());
        Integer processedAtCount = db.sql("SELECT COUNT(*) AS c FROM messages WHERE status='PROCESSED' "
                        + "AND container_id='w1' AND processed_at IS NOT NULL")
                .map((row, md) -> row.get("c", Integer.class))
                .one()
                .block();
        assertThat(processedAtCount).isEqualTo(2);
        Long third = repo.claimBatch("w1", 10, 5).block();
        assertThat(third).isEqualTo(1L);
    }

    @Test
    void fetchAndRetryTransitions_work() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        Long claimed = repo.claimBatch("w1", 10, 5).block();
        assertThat(claimed).isEqualTo(1L);

        var rows = repo.fetchClaimedForWorker("w1").collectList().block();
        assertThat(rows).hasSize(1);

        long id = rows.getFirst().id();
        repo.markReadyForRetry(id, 1).block();
        Integer retry = db.sql("SELECT retry_count AS c FROM messages WHERE id=" + id)
                .map((r, m) -> r.get("c", Integer.class))
                .one()
                .block();
        assertThat(retry).isEqualTo(1);

        repo.markStopped(id, 2).block();
        String status = db.sql("SELECT status FROM messages WHERE id=" + id)
                .map((r, m) -> r.get("status", String.class))
                .one()
                .block();
        assertThat(status).isEqualTo("STOPPED");
    }

    private static void resetData() {
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
    }

    private static void insert(String uuid, String key, String content, String status) {
        db.sql("INSERT INTO messages(uuid,key,content,status) VALUES (:u,:k,:c,:s)")
                .bind("u", uuid)
                .bind("k", key)
                .bind("c", content)
                .bind("s", status)
                .fetch()
                .rowsUpdated()
                .block();
    }
}
