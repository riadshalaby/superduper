package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcWorkerMessageRepositoryTest {

    @Test
    void usesPostgresWorkerSql() {
        JdbcSqlDialect dialect = new PostgresJdbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();
        String releaseSql = dialect.releaseMessagesSql();

        assertThat(claimSql).contains("FOR UPDATE OF m1 SKIP LOCKED");
        assertThat(claimSql).contains("p.message_key = m1.message_key");
        assertThat(claimSql).doesNotContain("NOT EXISTS (SELECT 1 FROM");
        assertThat(claimSql).doesNotContain("`key`");

        assertThat(fetchSql).contains("message_key");
        assertThat(fetchSql).contains("correlation_id");
        assertThat(fetchSql).contains("message_type");
        assertThat(fetchSql).doesNotContain("`key`");
        assertThat(releaseSql).contains("WHERE id IN (:ids) AND container_id=:cid");
    }

    @Test
    void usesMariaDbWorkerSql() {
        JdbcSqlDialect dialect = new MariaDbJdbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();
        String releaseSql = dialect.releaseMessagesSql();

        assertThat(claimSql).contains("FOR UPDATE SKIP LOCKED");
        assertThat(claimSql).doesNotContain("FOR UPDATE OF m1 SKIP LOCKED");
        assertThat(claimSql).contains("p.message_key = m1.message_key");

        assertThat(fetchSql).contains("message_key");
        assertThat(fetchSql).contains("ORDER BY message_key, id");
        assertThat(releaseSql).contains("WHERE id IN (:ids) AND container_id=:cid");
    }
}
