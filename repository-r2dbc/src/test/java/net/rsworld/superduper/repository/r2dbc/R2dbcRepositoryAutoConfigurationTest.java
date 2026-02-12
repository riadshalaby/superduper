package net.rsworld.superduper.repository.r2dbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class R2dbcRepositoryAutoConfigurationTest {

    private final R2dbcRepositoryAutoConfiguration cfg = new R2dbcRepositoryAutoConfiguration();

    @Test
    void sqlDialect_detectsFromProperty() {
        assertThat(cfg.r2dbcSqlDialect("postgres", "")).isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.r2dbcSqlDialect("postgresql", "")).isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.r2dbcSqlDialect("mariadb", "")).isEqualTo(SqlDialect.MARIADB);
        assertThat(cfg.r2dbcSqlDialect("maria", "")).isEqualTo(SqlDialect.MARIADB);
    }

    @Test
    void sqlDialect_detectsFromR2dbcUrl() {
        assertThat(cfg.r2dbcSqlDialect("", "r2dbc:postgresql://localhost:5432/test"))
                .isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.r2dbcSqlDialect("", "r2dbc:mariadb://localhost:3306/test"))
                .isEqualTo(SqlDialect.MARIADB);
    }

    @Test
    void sqlDialect_rejectsUnsupportedValues() {
        assertThatThrownBy(() -> cfg.r2dbcSqlDialect("oracle", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported superduper.db.dialect");

        assertThatThrownBy(() -> cfg.r2dbcSqlDialect("", "r2dbc:h2:mem:///test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to detect SQL dialect");
    }
}
