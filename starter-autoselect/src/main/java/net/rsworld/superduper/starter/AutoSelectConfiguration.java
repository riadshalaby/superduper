package net.rsworld.superduper.starter;

import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@EnableScheduling
public class AutoSelectConfiguration {

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public LockProvider lockProvider(DataSource ds) {
        return new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withTableName("shedlock")
                .withJdbcTemplate(new JdbcTemplate(ds))
                .build());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public LockingTaskExecutor lockingTaskExecutor(LockProvider lp) {
        return new DefaultLockingTaskExecutor(lp);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnBean({NamedParameterJdbcTemplate.class, MessageHandler.class, LockingTaskExecutor.class})
    public SuperDuperWorkerService jdbcWorker(
            NamedParameterJdbcTemplate jdbc,
            org.springframework.transaction.PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler) {
        return new SuperDuperWorkerService(jdbc, txm, lockExec, handler, 100, 5);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnBean({DatabaseClient.class, ReactiveMessageHandler.class})
    public SuperDuperWorkerReactiveService reactiveWorker(DatabaseClient db, ReactiveMessageHandler handler) {
        return new SuperDuperWorkerReactiveService(db, handler, 100, 5, 5000, 30000, 120000);
    }
}
