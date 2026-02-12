package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;

class R2dbcMessageIngestRepositoryTest {

    @Test
    void usesPostgresUpsertSyntax() throws Exception {
        R2dbcMessageIngestRepository repo =
                new R2dbcMessageIngestRepository(org.mockito.Mockito.mock(DatabaseClient.class), SqlDialect.POSTGRES);

        String sql = readUpsertSql(repo);
        assertThat(sql).contains("ON CONFLICT (uuid)");
        assertThat(sql).doesNotContain("ON DUPLICATE KEY UPDATE");
    }

    @Test
    void usesMariaDbUpsertSyntax() throws Exception {
        R2dbcMessageIngestRepository repo =
                new R2dbcMessageIngestRepository(org.mockito.Mockito.mock(DatabaseClient.class), SqlDialect.MARIADB);

        String sql = readUpsertSql(repo);
        assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("`key`");
    }

    private static String readUpsertSql(R2dbcMessageIngestRepository repo) throws Exception {
        Field f = R2dbcMessageIngestRepository.class.getDeclaredField("upsertSql");
        f.setAccessible(true);
        return (String) f.get(repo);
    }
}
