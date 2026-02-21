package net.rsworld.superduper.repository.jdbc;

import java.util.Locale;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@AutoConfiguration
public class JdbcRepositoryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SqlDialect sqlDialect(
            @Value("${superduper.db.dialect:}") String configuredDialect,
            @Value("${spring.datasource.url:}") String datasourceUrl) {
        if (!configuredDialect.isBlank()) {
            String raw = configuredDialect.trim().toUpperCase(Locale.ROOT);
            if ("POSTGRES".equals(raw) || "POSTGRESQL".equals(raw)) {
                return SqlDialect.POSTGRES;
            }
            if ("MARIADB".equals(raw) || "MARIA".equals(raw)) {
                return SqlDialect.MARIADB;
            }
            throw new IllegalArgumentException("Unsupported superduper.db.dialect: " + configuredDialect);
        }

        String url = datasourceUrl.toLowerCase(Locale.ROOT);
        if (url.contains("postgresql")) {
            return SqlDialect.POSTGRES;
        }
        if (url.contains("mariadb")) {
            return SqlDialect.MARIADB;
        }
        throw new IllegalArgumentException("Unable to detect SQL dialect. Set superduper.db.dialect=postgres|mariadb");
    }

    @Bean
    @ConditionalOnMissingBean
    public JdbcSqlDialect jdbcSqlDialect(SqlDialect dialect) {
        return switch (dialect) {
            case POSTGRES -> new PostgresJdbcSqlDialect();
            case MARIADB -> new MariaDbJdbcSqlDialect();
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageIngestRepository messageIngestRepository(NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        return new JdbcMessageIngestRepository(jdbc, dialect);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerMessageRepository workerMessageRepository(NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        return new JdbcWorkerMessageRepository(jdbc, dialect);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerMaintenanceRepository workerMaintenanceRepository(
            NamedParameterJdbcTemplate jdbc, JdbcSqlDialect dialect) {
        return new JdbcWorkerMaintenanceRepository(jdbc, dialect);
    }
}
