package net.rsworld.superduper.repository.r2dbc;

final class R2dbcSqlDialects {
    private R2dbcSqlDialects() {}

    static R2dbcSqlDialect from(SqlDialect dialect) {
        return from(dialect, new R2dbcTableProperties());
    }

    static R2dbcSqlDialect from(SqlDialect dialect, R2dbcTableProperties tables) {
        return switch (dialect) {
            case POSTGRES -> new PostgresR2dbcSqlDialect(tables.getMessages(), tables.getHeartbeats());
            case MARIADB -> new MariaDbR2dbcSqlDialect(tables.getMessages(), tables.getHeartbeats());
        };
    }
}
