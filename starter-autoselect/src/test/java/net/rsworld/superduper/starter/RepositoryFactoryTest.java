package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import net.rsworld.superduper.repository.jdbc.JdbcMessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.JdbcTableProperties;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMessageRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcTableProperties;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

class RepositoryFactoryTest {

    @Test
    void createsJdbcRepositoriesFromAvailableDependencies() {
        RepositoryFactory factory = new RepositoryFactory(
                provider(mock(NamedParameterJdbcTemplate.class)),
                provider(null),
                provider(null),
                provider(new JdbcTableProperties()),
                provider(null),
                provider(net.rsworld.superduper.repository.jdbc.SqlDialect.POSTGRES),
                provider(null));

        assertThat(factory.createIngestRepository("messages")).isInstanceOf(JdbcMessageIngestRepository.class);
        assertThat(factory.createWorkerRepository("messages")).isInstanceOf(JdbcWorkerMessageRepository.class);
        assertThat(factory.createMaintenanceRepository("messages")).isInstanceOf(JdbcWorkerMaintenanceRepository.class);
    }

    @Test
    void createsReactiveRepositoriesFromAvailableDependencies() {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        ConnectionFactoryMetadata metadata = mock(ConnectionFactoryMetadata.class);
        when(databaseClient.getConnectionFactory()).thenReturn(connectionFactory);
        when(connectionFactory.getMetadata()).thenReturn(metadata);
        when(metadata.getName()).thenReturn("PostgreSQL");

        RepositoryFactory factory = new RepositoryFactory(
                provider(null),
                provider(databaseClient),
                provider(mock(TransactionalOperator.class)),
                provider(null),
                provider(new R2dbcTableProperties()),
                provider(null),
                provider(net.rsworld.superduper.repository.r2dbc.SqlDialect.POSTGRES));

        assertThat(factory.createReactiveIngestRepository("messages")).isInstanceOf(R2dbcMessageIngestRepository.class);
        assertThat(factory.createReactiveWorkerRepository("messages")).isInstanceOf(R2dbcWorkerMessageRepository.class);
        assertThat(factory.createReactiveMaintenanceRepository("messages"))
                .isInstanceOf(R2dbcWorkerMaintenanceRepository.class);
    }

    @Test
    void failsFastWhenRequiredDependenciesAreMissing() {
        RepositoryFactory missingJdbc = new RepositoryFactory(
                provider(null),
                provider(null),
                provider(null),
                provider(null),
                provider(null),
                provider(net.rsworld.superduper.repository.jdbc.SqlDialect.POSTGRES),
                provider(null));
        RepositoryFactory missingDatabaseClient = new RepositoryFactory(
                provider(mock(NamedParameterJdbcTemplate.class)),
                provider(null),
                provider(mock(TransactionalOperator.class)),
                provider(null),
                provider(new R2dbcTableProperties()),
                provider(net.rsworld.superduper.repository.jdbc.SqlDialect.POSTGRES),
                provider(net.rsworld.superduper.repository.r2dbc.SqlDialect.MARIADB));
        RepositoryFactory missingTransactionalOperator = new RepositoryFactory(
                provider(mock(NamedParameterJdbcTemplate.class)),
                provider(mock(DatabaseClient.class)),
                provider(null),
                provider(null),
                provider(new R2dbcTableProperties()),
                provider(net.rsworld.superduper.repository.jdbc.SqlDialect.POSTGRES),
                provider(net.rsworld.superduper.repository.r2dbc.SqlDialect.MARIADB));

        assertThatThrownBy(() -> missingJdbc.createWorkerRepository("messages"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NamedParameterJdbcTemplate");
        assertThatThrownBy(() -> missingDatabaseClient.createReactiveIngestRepository("messages"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DatabaseClient");
        assertThatThrownBy(() -> missingTransactionalOperator.createReactiveWorkerRepository("messages"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TransactionalOperator");
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T value) {
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(value);
        when(provider.getIfAvailable(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            java.util.function.Supplier<T> supplier = invocation.getArgument(0);
            return value != null ? value : supplier.get();
        });
        return provider;
    }
}
