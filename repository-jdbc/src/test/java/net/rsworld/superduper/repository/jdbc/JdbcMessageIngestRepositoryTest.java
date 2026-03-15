package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import net.rsworld.superduper.repository.api.MessageIngestData;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

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

    @Test
    void batchUpsertReadyMessages_buildsBatchParameterSources() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.batchUpdate(any(), any(SqlParameterSource[].class))).thenReturn(new int[] {1, 1});
        JdbcMessageIngestRepository repository = new JdbcMessageIngestRepository(jdbc, SqlDialect.POSTGRES);
        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");

        repository.batchUpsertReadyMessages(List.of(
                new MessageIngestData("orders", "id-1", "key-1", "value-1", occurredAt, "corr-1", "order.created"),
                new MessageIngestData("orders", "id-2", "key-2", "value-2", occurredAt, null, null)));

        ArgumentCaptor<SqlParameterSource[]> paramsCaptor = ArgumentCaptor.forClass(SqlParameterSource[].class);
        verify(jdbc).batchUpdate(eq(new PostgresJdbcSqlDialect().upsertReadyMessageSql()), paramsCaptor.capture());

        SqlParameterSource[] parameterSources = paramsCaptor.getValue();
        assertThat(parameterSources).hasSize(2);
        assertThat(parameterSources[0].getValue("topic")).isEqualTo("orders");
        assertThat(parameterSources[0].getValue("messageId")).isEqualTo("id-1");
        assertThat(parameterSources[0].getValue("messageKey")).isEqualTo("key-1");
        assertThat(parameterSources[0].getValue("content")).isEqualTo("value-1");
        assertThat(parameterSources[0].getValue("occurredAt")).isEqualTo(Timestamp.from(occurredAt));
        assertThat(parameterSources[0].getValue("correlationId")).isEqualTo("corr-1");
        assertThat(parameterSources[0].getValue("messageType")).isEqualTo("order.created");
        assertThat(parameterSources[1].getValue("messageId")).isEqualTo("id-2");
        assertThat(parameterSources[1].getValue("correlationId")).isNull();
        assertThat(parameterSources[1].getValue("messageType")).isNull();
    }
}
