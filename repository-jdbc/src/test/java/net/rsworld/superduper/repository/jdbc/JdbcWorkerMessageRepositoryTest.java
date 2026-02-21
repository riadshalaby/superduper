package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcWorkerMessageRepositoryTest {

    @Test
    void usesPostgresWorkerSql() {
        JdbcSqlDialect dialect = new PostgresJdbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedSql();

        assertThat(claimSql).contains("FOR UPDATE OF m1 SKIP LOCKED");
        assertThat(claimSql).contains("p.key = m1.key");
        assertThat(claimSql).doesNotContain("`key`");

        assertThat(fetchSql).contains("key AS message_key");
        assertThat(fetchSql).doesNotContain("`key`");
    }

    @Test
    void usesMariaDbWorkerSql() {
        JdbcSqlDialect dialect = new MariaDbJdbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedSql();

        assertThat(claimSql).contains("FOR UPDATE SKIP LOCKED");
        assertThat(claimSql).doesNotContain("FOR UPDATE OF m1 SKIP LOCKED");
        assertThat(claimSql).contains("p.`key` = m1.`key`");

        assertThat(fetchSql).contains("`key` AS message_key");
        assertThat(fetchSql).contains("ORDER BY `key`, id");
    }
}
