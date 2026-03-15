package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import net.rsworld.superduper.repository.api.MessageIngestData;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class R2dbcMessageIngestRepositoryTest {

    @Test
    void usesPostgresUpsertSyntax() {
        String sql = new PostgresR2dbcSqlDialect().upsertReadyMessageSql();
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
        String sql = new MariaDbR2dbcSqlDialect().upsertReadyMessageSql();
        assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
        assertThat(sql).contains("message_key");
        assertThat(sql).contains("occurred_at");
        assertThat(sql).contains("received_at");
        assertThat(sql).contains("correlation_id");
        assertThat(sql).contains("message_type");
    }

    @Test
    @SuppressWarnings("unchecked")
    void batchUpsertReadyMessages_usesDriverBatchStatement() {
        DatabaseClient db = mock(DatabaseClient.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        Result result = mock(Result.class);
        when(db.getConnectionFactory())
                .thenReturn(new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                        .host("localhost")
                        .port(5432)
                        .database("superduper")
                        .username("superduper")
                        .password("superduper")
                        .build()));
        when(connection.createStatement(any())).thenReturn(statement);
        when(statement.bind(anyInt(), any())).thenReturn(statement);
        when(statement.bindNull(anyInt(), any())).thenReturn(statement);
        when(statement.add()).thenReturn(statement);
        doReturn(Flux.just(result)).when(statement).execute();
        when(result.getRowsUpdated()).thenReturn(Mono.just(2L));
        when(db.inConnectionMany(any()))
                .thenAnswer(
                        invocation -> ((Function<Connection, Flux<Long>>) invocation.getArgument(0)).apply(connection));

        R2dbcMessageIngestRepository repository = new R2dbcMessageIngestRepository(db, SqlDialect.POSTGRES);
        Instant occurredAt = Instant.parse("2026-02-21T12:00:00Z");

        repository
                .batchUpsertReadyMessages(List.of(
                        new MessageIngestData(
                                "orders", "id-1", "key-1", "value-1", occurredAt, "corr-1", "order.created"),
                        new MessageIngestData("orders", "id-2", "key-2", "value-2", occurredAt, null, null)))
                .block();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(connection).createStatement(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue()).doesNotContain(":topic");
        assertThat(sqlCaptor.getValue()).contains("$1");
        verify(statement).add();
        verify(statement, atLeastOnce()).bind(anyInt(), any());
        verify(statement).execute();
    }
}
