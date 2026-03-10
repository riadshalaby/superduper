package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.rsworld.superduper.schema.liquibase.test.LiquibaseTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mariadb.MariaDBContainer;

class JdbcWorkerMessageRepositoryMariaDbIntegrationTest {

    static MariaDBContainer mariadb;
    static NamedParameterJdbcTemplate jdbc;
    static JdbcWorkerMessageRepository repo;

    @BeforeAll
    static void beforeAll() {
        mariadb = new MariaDBContainer("mariadb:11.4");
        mariadb.start();

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUrl(mariadb.getJdbcUrl());
        ds.setUsername(mariadb.getUsername());
        ds.setPassword(mariadb.getPassword());

        jdbc = new NamedParameterJdbcTemplate(ds);
        repo = new JdbcWorkerMessageRepository(jdbc, SqlDialect.MARIADB);
        LiquibaseTestSupport.migrate(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterAll
    static void afterAll() {
        if (mariadb != null) {
            mariadb.stop();
        }
    }

    @Test
    void claimBatch_respectsPerKeyOrdering_onMariaDb() {
        resetData();
        insert("u1", "k1", "v1", "READY");
        insert("u2", "k1", "v2", "READY");
        insert("u3", "k2", "v3", "READY");

        long first = repo.claimBatch("w1", 10, 5);
        assertThat(first).isEqualTo(3);

        long second = repo.claimBatch("w1", 10, 5);
        assertThat(second).isZero();

        repo.fetchClaimedForWorker("w1")
                .forEach(row -> assertThat(repo.markProcessed(row.id(), "w1")).isTrue());
        long third = repo.claimBatch("w1", 10, 5);
        assertThat(third).isZero();
    }

    @Test
    void fetchAndFailureTransitions_work_onMariaDb() {
        resetData();
        insert("u4", "k4", "v4", "READY");

        long claimed = repo.claimBatch("w1", 10, 5);
        assertThat(claimed).isEqualTo(1);

        var rows = repo.fetchClaimedForWorker("w1");
        assertThat(rows).hasSize(1);

        long id = rows.getFirst().id();
        boolean failed = repo.markFailed(id, 1, "w1");
        assertThat(failed).isTrue();
        String failedStatus =
                jdbc.getJdbcTemplate().queryForObject("SELECT status FROM messages WHERE id=" + id, String.class);
        assertThat(failedStatus).isEqualTo("FAILED");
        Integer retry =
                jdbc.getJdbcTemplate().queryForObject("SELECT retry_count FROM messages WHERE id=" + id, Integer.class);
        assertThat(retry).isEqualTo(1);

        long reclaimed = repo.claimBatch("w2", 10, 5);
        assertThat(reclaimed).isEqualTo(1L);

        boolean staleUpdateAccepted = repo.markStopped(id, 2, "w1");
        assertThat(staleUpdateAccepted).isFalse();

        boolean stopByOwner = repo.markStopped(id, 2, "w2");
        assertThat(stopByOwner).isTrue();
        String status =
                jdbc.getJdbcTemplate().queryForObject("SELECT status FROM messages WHERE id=" + id, String.class);
        assertThat(status).isEqualTo("STOPPED");
    }

    @Test
    void claimBatch_isolatesKeysPerTopic_onMariaDb() {
        resetData();
        insert("a1", "shared-key", "va1", "READY", "topic-a");
        insert("a2", "shared-key", "va2", "READY", "topic-a");
        insert("b1", "shared-key", "vb1", "READY", "topic-b");
        insert("b2", "shared-key", "vb2", "READY", "topic-b");

        long claimedTopicA = repo.claimBatch("w-topic-a", 10, 5, "topic-a");
        long claimedTopicB = repo.claimBatch("w-topic-b", 10, 5, "topic-b");

        assertThat(claimedTopicA).isEqualTo(2);
        assertThat(claimedTopicB).isEqualTo(2);
        assertThat(repo.fetchClaimedForWorker("w-topic-a", "topic-a"))
                .extracting(row -> row.topic() + ":" + row.messageId())
                .containsExactly("topic-a:a1", "topic-a:a2");
        assertThat(repo.fetchClaimedForWorker("w-topic-b", "topic-b"))
                .extracting(row -> row.topic() + ":" + row.messageId())
                .containsExactly("topic-b:b1", "topic-b:b2");
    }

    @Test
    void dedicatedTableRows_areInvisibleToSharedTableQueries_onMariaDb() {
        resetData();
        recreateTopicTable();

        JdbcWorkerMessageRepository dedicatedRepo = new JdbcWorkerMessageRepository(
                jdbc, new MariaDbJdbcSqlDialect("orders_messages", "container_heartbeats"));

        insert("shared-1", "shared-key", "shared-value", "READY", "orders-topic");
        insertIntoTable("orders_messages", "dedicated-1", "dedicated-key", "dedicated-value", "READY", "orders-topic");

        assertThat(repo.findByStatus("READY", 10, "orders-topic"))
                .extracting(row -> row.messageId() + ":" + row.topic())
                .containsExactly("shared-1:orders-topic");
        assertThat(dedicatedRepo.findByStatus("READY", 10, "orders-topic"))
                .extracting(row -> row.messageId() + ":" + row.topic())
                .containsExactly("dedicated-1:orders-topic");

        assertThat(repo.claimBatch("shared-worker", 10, 5, "orders-topic")).isEqualTo(1);
        assertThat(dedicatedRepo.claimBatch("dedicated-worker", 10, 5, "orders-topic"))
                .isEqualTo(1);
        assertThat(repo.fetchClaimedForWorker("shared-worker", "orders-topic"))
                .extracting(row -> row.messageId())
                .containsExactly("shared-1");
        assertThat(dedicatedRepo.fetchClaimedForWorker("dedicated-worker", "orders-topic"))
                .extracting(row -> row.messageId())
                .containsExactly("dedicated-1");
    }

    private static void resetData() {
        jdbc.getJdbcTemplate().execute("TRUNCATE TABLE messages");
    }

    private static void insert(String messageId, String messageKey, String content, String status) {
        insert(messageId, messageKey, content, status, "default");
    }

    private static void insert(String messageId, String messageKey, String content, String status, String topic) {
        insertIntoTable("messages", messageId, messageKey, content, status, topic);
    }

    private static void insertIntoTable(
            String tableName, String messageId, String messageKey, String content, String status, String topic) {
        jdbc.getJdbcTemplate()
                .update(
                        "INSERT INTO " + tableName + "(topic,message_id,message_key,content,status) VALUES (?,?,?,?,?)",
                        topic,
                        messageId,
                        messageKey,
                        content,
                        status);
    }

    private static void recreateTopicTable() {
        jdbc.getJdbcTemplate().execute("DROP TABLE IF EXISTS orders_messages");
        jdbc.getJdbcTemplate()
                .execute("CREATE TABLE orders_messages ("
                        + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                        + "topic VARCHAR(255) NOT NULL DEFAULT 'default',"
                        + "message_id VARCHAR(36) UNIQUE NOT NULL,"
                        + "message_key VARCHAR(255) NOT NULL,"
                        + "content TEXT,"
                        + "status VARCHAR(32) NOT NULL,"
                        + "retry_count INT DEFAULT 0,"
                        + "container_id VARCHAR(255),"
                        + "correlation_id VARCHAR(36) NULL,"
                        + "message_type VARCHAR(255) NULL,"
                        + "occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                        + "received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                        + "processed_at TIMESTAMP NULL,"
                        + "last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        jdbc.getJdbcTemplate()
                .execute("CREATE INDEX idx_orders_messages_topic_status_key_id "
                        + "ON orders_messages (topic, status, message_key, id)");
        jdbc.getJdbcTemplate()
                .execute("CREATE INDEX idx_orders_messages_processing_worker_status_container_key_id "
                        + "ON orders_messages (status, container_id(191), topic(191), message_key(191), id)");
        jdbc.getJdbcTemplate()
                .execute("CREATE INDEX idx_orders_messages_processing_stale_status_last_updated "
                        + "ON orders_messages (topic, status, last_updated)");
    }
}
