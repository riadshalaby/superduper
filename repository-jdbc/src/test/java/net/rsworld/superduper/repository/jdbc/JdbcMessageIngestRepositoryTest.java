package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcMessageIngestRepositoryTest {

    @Test
    void usesPostgresUpsertSyntax() {
        String sql = new PostgresJdbcSqlDialect().upsertReadyMessageSql();
        assertThat(sql).contains("ON CONFLICT (uuid)");
        assertThat(sql).doesNotContain("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
    }

    @Test
    void usesMariaDbUpsertSyntax() {
        String sql = new MariaDbJdbcSqlDialect().upsertReadyMessageSql();
        assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("`key`");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
    }
}
