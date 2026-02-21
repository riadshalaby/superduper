package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

class JdbcMessageIngestRepositoryTest {

    @Test
    void usesPostgresUpsertSyntax() throws Exception {
        JdbcMessageIngestRepository repo = new JdbcMessageIngestRepository(
                org.mockito.Mockito.mock(NamedParameterJdbcTemplate.class), SqlDialect.POSTGRES);

        String sql = readUpsertSql(repo);
        assertThat(sql).contains("ON CONFLICT (uuid)");
        assertThat(sql).doesNotContain("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
    }

    @Test
    void usesMariaDbUpsertSyntax() throws Exception {
        JdbcMessageIngestRepository repo = new JdbcMessageIngestRepository(
                org.mockito.Mockito.mock(NamedParameterJdbcTemplate.class), SqlDialect.MARIADB);

        String sql = readUpsertSql(repo);
        assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("`key`");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
    }

    private static String readUpsertSql(JdbcMessageIngestRepository repo) throws Exception {
        Field f = JdbcMessageIngestRepository.class.getDeclaredField("upsertSql");
        f.setAccessible(true);
        return (String) f.get(repo);
    }
}
