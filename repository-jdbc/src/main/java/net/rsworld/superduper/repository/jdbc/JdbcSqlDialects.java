package net.rsworld.superduper.repository.jdbc;

final class JdbcSqlDialects {
    private JdbcSqlDialects() {}

    static JdbcSqlDialect from(SqlDialect dialect) {
        return switch (dialect) {
            case POSTGRES -> new PostgresJdbcSqlDialect();
            case MARIADB -> new MariaDbJdbcSqlDialect();
        };
    }
}
