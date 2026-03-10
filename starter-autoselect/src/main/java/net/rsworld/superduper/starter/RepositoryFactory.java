package net.rsworld.superduper.starter;

import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.JdbcMessageIngestRepository;
import net.rsworld.superduper.repository.jdbc.JdbcSqlDialect;
import net.rsworld.superduper.repository.jdbc.JdbcTableProperties;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.jdbc.JdbcWorkerMessageRepository;
import net.rsworld.superduper.repository.jdbc.MariaDbJdbcSqlDialect;
import net.rsworld.superduper.repository.jdbc.PostgresJdbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.MariaDbR2dbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.PostgresR2dbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.R2dbcMessageIngestRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcSqlDialect;
import net.rsworld.superduper.repository.r2dbc.R2dbcTableProperties;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.r2dbc.R2dbcWorkerMessageRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

public class RepositoryFactory implements TopicRepositoryFactory {
    private final ObjectProvider<NamedParameterJdbcTemplate> jdbcProvider;
    private final ObjectProvider<DatabaseClient> databaseClientProvider;
    private final ObjectProvider<TransactionalOperator> transactionalOperatorProvider;
    private final ObjectProvider<JdbcTableProperties> jdbcTablePropertiesProvider;
    private final ObjectProvider<R2dbcTableProperties> r2dbcTablePropertiesProvider;
    private final ObjectProvider<net.rsworld.superduper.repository.jdbc.SqlDialect> jdbcDialectProvider;
    private final ObjectProvider<net.rsworld.superduper.repository.r2dbc.SqlDialect> r2dbcDialectProvider;

    public RepositoryFactory(
            ObjectProvider<NamedParameterJdbcTemplate> jdbcProvider,
            ObjectProvider<DatabaseClient> databaseClientProvider,
            ObjectProvider<TransactionalOperator> transactionalOperatorProvider,
            ObjectProvider<JdbcTableProperties> jdbcTablePropertiesProvider,
            ObjectProvider<R2dbcTableProperties> r2dbcTablePropertiesProvider,
            ObjectProvider<net.rsworld.superduper.repository.jdbc.SqlDialect> jdbcDialectProvider,
            ObjectProvider<net.rsworld.superduper.repository.r2dbc.SqlDialect> r2dbcDialectProvider) {
        this.jdbcProvider = jdbcProvider;
        this.databaseClientProvider = databaseClientProvider;
        this.transactionalOperatorProvider = transactionalOperatorProvider;
        this.jdbcTablePropertiesProvider = jdbcTablePropertiesProvider;
        this.r2dbcTablePropertiesProvider = r2dbcTablePropertiesProvider;
        this.jdbcDialectProvider = jdbcDialectProvider;
        this.r2dbcDialectProvider = r2dbcDialectProvider;
    }

    @Override
    public MessageIngestRepository createIngestRepository(String tableName) {
        return new JdbcMessageIngestRepository(requiredJdbc(), jdbcDialect(tableName));
    }

    @Override
    public WorkerMessageRepository createWorkerRepository(String tableName) {
        return new JdbcWorkerMessageRepository(requiredJdbc(), jdbcDialect(tableName));
    }

    @Override
    public WorkerMaintenanceRepository createMaintenanceRepository(String tableName) {
        return new JdbcWorkerMaintenanceRepository(requiredJdbc(), jdbcDialect(tableName));
    }

    @Override
    public ReactiveMessageIngestRepository createReactiveIngestRepository(String tableName) {
        return new R2dbcMessageIngestRepository(requiredDatabaseClient(), r2dbcDialect(tableName));
    }

    @Override
    public ReactiveWorkerMessageRepository createReactiveWorkerRepository(String tableName) {
        return new R2dbcWorkerMessageRepository(
                requiredDatabaseClient(), requiredTransactionalOperator(), r2dbcDialect(tableName));
    }

    @Override
    public ReactiveWorkerMaintenanceRepository createReactiveMaintenanceRepository(String tableName) {
        return new R2dbcWorkerMaintenanceRepository(requiredDatabaseClient(), r2dbcDialect(tableName));
    }

    private NamedParameterJdbcTemplate requiredJdbc() {
        NamedParameterJdbcTemplate jdbc = jdbcProvider.getIfAvailable();
        if (jdbc == null) {
            throw new IllegalStateException("NamedParameterJdbcTemplate is not available");
        }
        return jdbc;
    }

    private DatabaseClient requiredDatabaseClient() {
        DatabaseClient databaseClient = databaseClientProvider.getIfAvailable();
        if (databaseClient == null) {
            throw new IllegalStateException("DatabaseClient is not available");
        }
        return databaseClient;
    }

    private TransactionalOperator requiredTransactionalOperator() {
        TransactionalOperator transactionalOperator = transactionalOperatorProvider.getIfAvailable();
        if (transactionalOperator == null) {
            throw new IllegalStateException("TransactionalOperator is not available");
        }
        return transactionalOperator;
    }

    private JdbcSqlDialect jdbcDialect(String tableName) {
        JdbcTableProperties tables = jdbcTablePropertiesProvider.getIfAvailable(JdbcTableProperties::new);
        return switch (requiredJdbcDialect()) {
            case POSTGRES -> new PostgresJdbcSqlDialect(tableName, tables.getHeartbeats());
            case MARIADB -> new MariaDbJdbcSqlDialect(tableName, tables.getHeartbeats());
        };
    }

    private R2dbcSqlDialect r2dbcDialect(String tableName) {
        R2dbcTableProperties tables = r2dbcTablePropertiesProvider.getIfAvailable(R2dbcTableProperties::new);
        return switch (requiredR2dbcDialect()) {
            case POSTGRES -> new PostgresR2dbcSqlDialect(tableName, tables.getHeartbeats());
            case MARIADB -> new MariaDbR2dbcSqlDialect(tableName, tables.getHeartbeats());
        };
    }

    private net.rsworld.superduper.repository.jdbc.SqlDialect requiredJdbcDialect() {
        net.rsworld.superduper.repository.jdbc.SqlDialect dialect = jdbcDialectProvider.getIfAvailable();
        if (dialect == null) {
            throw new IllegalStateException("JDBC SQL dialect is not available");
        }
        return dialect;
    }

    private net.rsworld.superduper.repository.r2dbc.SqlDialect requiredR2dbcDialect() {
        net.rsworld.superduper.repository.r2dbc.SqlDialect dialect = r2dbcDialectProvider.getIfAvailable();
        if (dialect == null) {
            throw new IllegalStateException("R2DBC SQL dialect is not available");
        }
        return dialect;
    }
}
