package net.rsworld.superduper.starter;

import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.ConnectionFactory;
import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.provider.r2dbc.R2dbcLockProvider;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.logging.LoggingSuperduperObserver;
import net.rsworld.superduper.observability.metrics.MetricsSuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.worker.blocking.CleanupService;
import net.rsworld.superduper.worker.blocking.HeartbeatService;
import net.rsworld.superduper.worker.blocking.MessageHandler;
import net.rsworld.superduper.worker.blocking.OrphanReclaimer;
import net.rsworld.superduper.worker.blocking.QueueHealthService;
import net.rsworld.superduper.worker.blocking.RedriveService;
import net.rsworld.superduper.worker.blocking.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveCleanupService;
import net.rsworld.superduper.worker.reactive.ReactiveHeartbeatService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.ReactiveOrphanReclaimer;
import net.rsworld.superduper.worker.reactive.ReactiveQueueHealthService;
import net.rsworld.superduper.worker.reactive.ReactiveRedriveService;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties({ObservabilityProperties.class, WorkerProperties.class})
public class AutoSelectConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConsumerMetadataResolver consumerMetadataResolver() {
        return new DefaultConsumerMetadataResolver();
    }

    @Bean
    @ConditionalOnMissingBean
    public SuperduperObserver superduperObserver(
            ObservabilityProperties properties,
            org.springframework.beans.factory.ObjectProvider<MeterRegistry> meterRegistry) {
        var settings = properties.toSettings();
        if (!settings.enabled()) {
            return NoopSuperduperObserver.INSTANCE;
        }
        if (settings.metricsEnabled()) {
            var registry = meterRegistry.getIfAvailable();
            if (registry != null) {
                return new MetricsSuperduperObserver(registry, settings);
            }
        }
        if (settings.logEnabled()) {
            return new LoggingSuperduperObserver(settings);
        }
        return NoopSuperduperObserver.INSTANCE;
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public LockProvider lockProvider(DataSource ds, WorkerProperties workerProperties) {
        return new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withTableName(workerProperties.getShedlock().getTableName())
                .withJdbcTemplate(new JdbcTemplate(ds))
                .build());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public LockProvider reactiveLockProvider(ConnectionFactory connectionFactory, WorkerProperties workerProperties) {
        return new R2dbcLockProvider(
                connectionFactory, workerProperties.getShedlock().getTableName());
    }

    @Bean
    @ConditionalOnMissingBean
    public LockingTaskExecutor lockingTaskExecutor(LockProvider lp) {
        return new DefaultLockingTaskExecutor(lp);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public SuperDuperWorkerService jdbcWorker(
            WorkerMessageRepository messageRepository,
            org.springframework.transaction.PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                observer,
                workerProperties.getBatchSize(),
                workerProperties.getMaxRetries(),
                workerProperties.getShedlock().getClaimLockName(),
                workerProperties.getShedlock().getLockAtMostForMs(),
                workerProperties.getShedlock().getLockAtLeastForMs());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public HeartbeatService jdbcHeartbeatService(
            WorkerMaintenanceRepository maintenanceRepository, SuperduperObserver observer) {
        return new HeartbeatService(maintenanceRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public OrphanReclaimer jdbcOrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new OrphanReclaimer(maintenanceRepository, observer, workerProperties.getOrphanTimeoutMs(), (int)
                workerProperties.getHeartbeatWindowMs());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public RedriveService jdbcRedriveService(WorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new RedriveService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnProperty(name = "superduper.worker.queue-health.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public QueueHealthService jdbcQueueHealthService(
            WorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new QueueHealthService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnProperty(name = "superduper.worker.retention.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public CleanupService jdbcCleanupService(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        WorkerProperties.Retention retention = workerProperties.getRetention();
        return new CleanupService(
                maintenanceRepository,
                observer,
                retention.getProcessedRetentionDays(),
                retention.getStoppedRetentionDays(),
                retention.getHeartbeatRetentionDays());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public SuperDuperWorkerReactiveService reactiveWorker(
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new SuperDuperWorkerReactiveService(
                messageRepository,
                lockExec,
                handler,
                observer,
                workerProperties.getBatchSize(),
                workerProperties.getMaxRetries(),
                workerProperties.getShedlock().getClaimLockName(),
                workerProperties.getShedlock().getLockAtMostForMs(),
                workerProperties.getShedlock().getLockAtLeastForMs());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public ReactiveHeartbeatService reactiveHeartbeatService(
            ReactiveWorkerMaintenanceRepository maintenanceRepository, SuperduperObserver observer) {
        return new ReactiveHeartbeatService(maintenanceRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public ReactiveOrphanReclaimer reactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new ReactiveOrphanReclaimer(maintenanceRepository, observer, workerProperties.getOrphanTimeoutMs(), (int)
                workerProperties.getHeartbeatWindowMs());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public ReactiveRedriveService reactiveRedriveService(
            ReactiveWorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new ReactiveRedriveService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnProperty(name = "superduper.worker.queue-health.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public ReactiveQueueHealthService reactiveQueueHealthService(
            ReactiveWorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new ReactiveQueueHealthService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnProperty(name = "superduper.worker.retention.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public ReactiveCleanupService reactiveCleanupService(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        WorkerProperties.Retention retention = workerProperties.getRetention();
        return new ReactiveCleanupService(
                maintenanceRepository,
                observer,
                retention.getProcessedRetentionDays(),
                retention.getStoppedRetentionDays(),
                retention.getHeartbeatRetentionDays());
    }
}
