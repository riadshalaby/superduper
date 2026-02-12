package net.rsworld.superduper.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class JdbcRepositoryAutoConfigurationTest {

    private final JdbcRepositoryAutoConfiguration cfg = new JdbcRepositoryAutoConfiguration();

    @Test
    void sqlDialect_detectsFromProperty() {
        assertThat(cfg.sqlDialect("postgres", "")).isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.sqlDialect("postgresql", "")).isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.sqlDialect("mariadb", "")).isEqualTo(SqlDialect.MARIADB);
        assertThat(cfg.sqlDialect("maria", "")).isEqualTo(SqlDialect.MARIADB);
    }

    @Test
    void sqlDialect_detectsFromJdbcUrl() {
        assertThat(cfg.sqlDialect("", "jdbc:postgresql://localhost:5432/test")).isEqualTo(SqlDialect.POSTGRES);
        assertThat(cfg.sqlDialect("", "jdbc:mariadb://localhost:3306/test")).isEqualTo(SqlDialect.MARIADB);
    }

    @Test
    void sqlDialect_rejectsUnsupportedValues() {
        assertThatThrownBy(() -> cfg.sqlDialect("oracle", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported superduper.db.dialect");

        assertThatThrownBy(() -> cfg.sqlDialect("", "jdbc:h2:mem:test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to detect SQL dialect");
    }
}
