package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class R2dbcWorkerMessageRepositoryTest {

    @Test
    void usesPostgresWorkerSql() {
        R2dbcSqlDialect dialect = new PostgresR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();

        assertThat(claimSql).contains("p.key = m1.key");
        assertThat(claimSql).doesNotContain("`key`");
        assertThat(fetchSql).contains("key AS message_key");
        assertThat(fetchSql).doesNotContain("`key`");
    }

    @Test
    void usesMariaDbWorkerSql() {
        R2dbcSqlDialect dialect = new MariaDbR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();

        assertThat(claimSql).contains("p.`key` = m1.`key`");
        assertThat(fetchSql).contains("`key` AS message_key");
        assertThat(fetchSql).contains("ORDER BY `key`, id");
    }
}
