package net.rsworld.superduper.repository.jdbc;

final class JdbcSqlDialects {
    private JdbcSqlDialects() {}

    static JdbcSqlDialect from(SqlDialect dialect) {
        return from(dialect, new JdbcTableProperties());
    }

    static JdbcSqlDialect from(SqlDialect dialect, JdbcTableProperties tables) {
        return switch (dialect) {
            case POSTGRES -> new PostgresJdbcSqlDialect(tables.getMessages(), tables.getHeartbeats());
            case MARIADB -> new MariaDbJdbcSqlDialect(tables.getMessages(), tables.getHeartbeats());
        };
    }
}
