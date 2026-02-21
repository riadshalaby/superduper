package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class R2dbcWorkerMessageRepositoryTest {

    @Test
    void usesPostgresWorkerSql() {
        R2dbcSqlDialect dialect = new PostgresR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchTemplate = dialect.fetchClaimedByIdsSqlTemplate();

        assertThat(claimSql).contains("p.key = m1.key");
        assertThat(claimSql).doesNotContain("`key`");
        assertThat(fetchTemplate).contains("key AS message_key");
        assertThat(fetchTemplate).doesNotContain("`key`");
    }

    @Test
    void usesMariaDbWorkerSql() {
        R2dbcSqlDialect dialect = new MariaDbR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchTemplate = dialect.fetchClaimedByIdsSqlTemplate();

        assertThat(claimSql).contains("p.`key` = m1.`key`");
        assertThat(fetchTemplate).contains("`key` AS message_key");
        assertThat(fetchTemplate).contains("ORDER BY `key`, id");
    }
}
