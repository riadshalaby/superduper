package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcMessageIngestRepositoryTest {

    @Test
    void usesPostgresUpsertSyntax() {
        String sql = new PostgresJdbcSqlDialect().upsertReadyMessageSql();
        assertThat(sql).contains("ON CONFLICT (message_id)");
        assertThat(sql).doesNotContain("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("message_key");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
        assertThat(sql).contains("correlation_id");
        assertThat(sql).contains("message_type");
    }

    @Test
    void usesMariaDbUpsertSyntax() {
        String sql = new MariaDbJdbcSqlDialect().upsertReadyMessageSql();
        assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("message_key");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
        assertThat(sql).contains("correlation_id");
        assertThat(sql).contains("message_type");
    }
}
