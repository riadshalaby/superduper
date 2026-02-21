package net.rsworld.superduper.repository.r2dbc;

final class R2dbcSqlDialects {
    private R2dbcSqlDialects() {}

    static R2dbcSqlDialect from(SqlDialect dialect) {
        return switch (dialect) {
            case POSTGRES -> new PostgresR2dbcSqlDialect();
            case MARIADB -> new MariaDbR2dbcSqlDialect();
        };
    }
}
