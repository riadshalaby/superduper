package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class R2dbcWorkerMessageRepositoryTest {

    @Test
    void usesPostgresWorkerSql() {
        R2dbcSqlDialect dialect = new PostgresR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();
        String findByStatusSql = dialect.findByStatusSql();
        String releaseSql = dialect.releaseMessagesSql();
        String redriveByIdSql = dialect.redriveByIdSql();
        String redriveByStatusSql = dialect.redriveByStatusSql();
        String countByStatusSql = dialect.countByStatusSql();

        assertThat(claimSql).contains("p.message_key = m1.message_key");
        assertThat(claimSql).doesNotContain("NOT EXISTS (SELECT 1 FROM");
        assertThat(claimSql).doesNotContain("`key`");
        assertThat(fetchSql).contains("message_key");
        assertThat(fetchSql).contains("correlation_id");
        assertThat(fetchSql).contains("message_type");
        assertThat(fetchSql).doesNotContain("`key`");
        assertThat(findByStatusSql).contains("WHERE status = :status AND topic = :topic ORDER BY id LIMIT :limit");
        assertThat(releaseSql).contains("WHERE id IN (:ids) AND container_id=:cid");
        assertThat(redriveByIdSql).contains("status IN ('FAILED', 'STOPPED')");
        assertThat(redriveByStatusSql)
                .contains("SELECT id FROM messages WHERE status = :status AND topic = :topic ORDER BY id LIMIT :limit");
        assertThat(countByStatusSql)
                .contains("SELECT status, COUNT(*) AS cnt FROM messages WHERE topic = :topic GROUP BY status");
    }

    @Test
    void usesMariaDbWorkerSql() {
        R2dbcSqlDialect dialect = new MariaDbR2dbcSqlDialect();
        String claimSql = dialect.claimBatchSql();
        String fetchSql = dialect.fetchClaimedForWorkerSql();
        String findByStatusSql = dialect.findByStatusSql();
        String releaseSql = dialect.releaseMessagesSql();
        String redriveByIdSql = dialect.redriveByIdSql();
        String redriveByStatusSql = dialect.redriveByStatusSql();
        String countByStatusSql = dialect.countByStatusSql();

        assertThat(claimSql).contains("p.message_key = m1.message_key");
        assertThat(fetchSql).contains("message_key");
        assertThat(fetchSql).contains("ORDER BY message_key, id");
        assertThat(findByStatusSql).contains("WHERE status = :status AND topic = :topic ORDER BY id LIMIT :limit");
        assertThat(releaseSql).contains("WHERE id IN (:ids) AND container_id=:cid");
        assertThat(redriveByIdSql).contains("status IN ('FAILED', 'STOPPED')");
        assertThat(redriveByStatusSql)
                .contains("JOIN (SELECT id FROM (SELECT id FROM messages WHERE status = :status AND topic = :topic");
        assertThat(countByStatusSql)
                .contains("SELECT status, COUNT(*) AS cnt FROM messages WHERE topic = :topic GROUP BY status");
    }
}
