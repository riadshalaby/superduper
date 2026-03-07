package net.rsworld.superduper.repository.r2dbc;

import java.util.Locale;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

@AutoConfiguration
@ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
@EnableConfigurationProperties(R2dbcTableProperties.class)
public class R2dbcRepositoryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SqlDialect r2dbcSqlDialect(
            @Value("${superduper.db.dialect:}") String configuredDialect,
            @Value("${spring.r2dbc.url:}") String r2dbcUrl) {
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

        String url = r2dbcUrl.toLowerCase(Locale.ROOT);
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
    public R2dbcSqlDialect r2dbcSqlDialectStrategy(SqlDialect dialect, R2dbcTableProperties tableProperties) {
        return R2dbcSqlDialects.from(dialect, tableProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveMessageIngestRepository reactiveMessageIngestRepository(DatabaseClient db, R2dbcSqlDialect dialect) {
        return new R2dbcMessageIngestRepository(db, dialect);
    }

    @Bean
    @ConditionalOnMissingBean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager txManager) {
        return TransactionalOperator.create(txManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveWorkerMessageRepository reactiveWorkerMessageRepository(
            DatabaseClient db, TransactionalOperator tx, R2dbcSqlDialect dialect) {
        return new R2dbcWorkerMessageRepository(db, tx, dialect);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveWorkerMaintenanceRepository reactiveWorkerMaintenanceRepository(
            DatabaseClient db, R2dbcSqlDialect dialect) {
        return new R2dbcWorkerMaintenanceRepository(db, dialect);
    }
}
