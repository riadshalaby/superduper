package net.rsworld.superduper.starter;

import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.ConnectionFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import net.rsworld.superduper.worker.blocking.TopicWorkerCoordinator;
import net.rsworld.superduper.worker.reactive.ReactiveCleanupService;
import net.rsworld.superduper.worker.reactive.ReactiveHeartbeatService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.ReactiveOrphanReclaimer;
import net.rsworld.superduper.worker.reactive.ReactiveQueueHealthService;
import net.rsworld.superduper.worker.reactive.ReactiveRedriveService;
import net.rsworld.superduper.worker.reactive.ReactiveTopicWorkerCoordinator;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties({ObservabilityProperties.class, TopicProperties.class, WorkerProperties.class})
public class AutoSelectConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConsumerMetadataResolver consumerMetadataResolver() {
        return new DefaultConsumerMetadataResolver();
    }

    @Bean
    @ConditionalOnMissingBean
    public SuperduperObserver superduperObserver(
            ObservabilityProperties properties, ObjectProvider<MeterRegistry> meterRegistry) {
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
    @ConditionalOnMissingBean
    public TaskScheduler superduperTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("superduper-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean
    public TopicRegistry topicRegistry(
            TopicProperties topicProperties,
            WorkerProperties workerProperties,
            @Value("${superduper.kafka.topic:}") String legacyTopic,
            @Value("${superduper.consumer.type:spring}") String consumerType,
            Map<String, MessageHandler> blockingHandlers,
            Map<String, ReactiveMessageHandler> reactiveHandlers) {
        if (!topicProperties.getTopics().isEmpty()) {
            List<TopicRegistry.ResolvedTopicConfig> topics = new ArrayList<>();
            for (Map.Entry<String, TopicProperties.TopicConfig> entry :
                    topicProperties.getTopics().entrySet()) {
                TopicProperties.TopicConfig config = entry.getValue();
                if (config.getKafkaTopic() == null || config.getKafkaTopic().isBlank()) {
                    throw new IllegalArgumentException(
                            "superduper.topics." + entry.getKey() + ".kafkaTopic must be set");
                }
                if (config.getHandler() == null || config.getHandler().isBlank()) {
                    throw new IllegalArgumentException("superduper.topics." + entry.getKey() + ".handler must be set");
                }
                topics.add(new TopicRegistry.ResolvedTopicConfig(
                        entry.getKey(),
                        config.getKafkaTopic(),
                        config.getHandler(),
                        config.getBatchSize() > 0 ? config.getBatchSize() : workerProperties.getBatchSize(),
                        config.getMaxRetries() > 0 ? config.getMaxRetries() : workerProperties.getMaxRetries(),
                        config.getTable(),
                        "superduper-claim-" + entry.getKey()));
            }
            return new TopicRegistry(topics);
        }
        if (legacyTopic == null || legacyTopic.isBlank()) {
            throw new IllegalArgumentException(
                    "No topic configured. Set superduper.kafka.topic or define superduper.topics.");
        }
        String handlerBeanName = resolveLegacyHandlerBeanName(consumerType, blockingHandlers, reactiveHandlers);
        return new TopicRegistry(List.of(new TopicRegistry.ResolvedTopicConfig(
                "default",
                legacyTopic,
                handlerBeanName,
                workerProperties.getBatchSize(),
                workerProperties.getMaxRetries(),
                "",
                "superduper-claim-default")));
    }

    private String resolveLegacyHandlerBeanName(
            String consumerType,
            Map<String, MessageHandler> blockingHandlers,
            Map<String, ReactiveMessageHandler> reactiveHandlers) {
        Map<String, ?> handlers = "reactor".equalsIgnoreCase(consumerType) ? reactiveHandlers : blockingHandlers;
        if (handlers.size() != 1) {
            throw new IllegalArgumentException(
                    "Single-topic mode requires exactly one handler bean. Found " + handlers.keySet());
        }
        return handlers.keySet().iterator().next();
    }

    @Bean
    @ConditionalOnMissingBean
    public RepositoryFactory repositoryFactory(
            ObjectProvider<org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate> jdbcProvider,
            ObjectProvider<org.springframework.r2dbc.core.DatabaseClient> databaseClientProvider,
            ObjectProvider<org.springframework.transaction.reactive.TransactionalOperator>
                    transactionalOperatorProvider,
            ObjectProvider<net.rsworld.superduper.repository.jdbc.JdbcTableProperties> jdbcTablePropertiesProvider,
            ObjectProvider<net.rsworld.superduper.repository.r2dbc.R2dbcTableProperties> r2dbcTablePropertiesProvider,
            ObjectProvider<net.rsworld.superduper.repository.jdbc.SqlDialect> jdbcDialectProvider,
            ObjectProvider<net.rsworld.superduper.repository.r2dbc.SqlDialect> r2dbcDialectProvider) {
        return new RepositoryFactory(
                jdbcProvider,
                databaseClientProvider,
                transactionalOperatorProvider,
                jdbcTablePropertiesProvider,
                r2dbcTablePropertiesProvider,
                jdbcDialectProvider,
                r2dbcDialectProvider);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public TopicWorkerCoordinator jdbcWorker(
            TopicRegistry topicRegistry,
            WorkerMessageRepository messageRepository,
            org.springframework.transaction.PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            Map<String, MessageHandler> handlers,
            SuperduperObserver observer,
            RepositoryFactory repositoryFactory,
            TaskScheduler superduperTaskScheduler,
            WorkerProperties workerProperties) {
        return new TopicWorkerCoordinator(
                topicRegistry,
                messageRepository,
                txm,
                lockExec,
                handlers,
                observer,
                repositoryFactory,
                superduperTaskScheduler,
                workerProperties.getClaimIntervalMs(),
                workerProperties.getClaimInitialDelayMs(),
                workerProperties.getShedlock().getLockAtMostForMs(),
                workerProperties.getShedlock().getLockAtLeastForMs());
    }

    SuperDuperWorkerService jdbcWorker(
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
                "superduper-claim-batch",
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
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new OrphanReclaimer(
                maintenanceRepository,
                topicRegistry,
                repositoryFactory,
                observer,
                workerProperties.getOrphanTimeoutMs(),
                (int) workerProperties.getHeartbeatWindowMs());
    }

    OrphanReclaimer jdbcOrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new OrphanReclaimer(maintenanceRepository, observer, workerProperties.getOrphanTimeoutMs(), (int)
                workerProperties.getHeartbeatWindowMs());
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public RedriveService jdbcRedriveService(
            WorkerMessageRepository messageRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer) {
        return new RedriveService(messageRepository, topicRegistry, repositoryFactory, observer);
    }

    RedriveService jdbcRedriveService(WorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new RedriveService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnProperty(name = "superduper.worker.queue-health.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public QueueHealthService jdbcQueueHealthService(
            WorkerMessageRepository messageRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer) {
        return new QueueHealthService(messageRepository, topicRegistry, repositoryFactory, observer);
    }

    QueueHealthService jdbcQueueHealthService(WorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new QueueHealthService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnProperty(name = "superduper.worker.retention.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public CleanupService jdbcCleanupService(
            WorkerMaintenanceRepository maintenanceRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        WorkerProperties.Retention retention = workerProperties.getRetention();
        return new CleanupService(
                maintenanceRepository,
                topicRegistry,
                repositoryFactory,
                observer,
                retention.getProcessedRetentionDays(),
                retention.getStoppedRetentionDays(),
                retention.getHeartbeatRetentionDays());
    }

    CleanupService jdbcCleanupService(
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
    public ReactiveTopicWorkerCoordinator reactiveWorker(
            TopicRegistry topicRegistry,
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            Map<String, ReactiveMessageHandler> handlers,
            SuperduperObserver observer,
            RepositoryFactory repositoryFactory,
            TaskScheduler superduperTaskScheduler,
            WorkerProperties workerProperties) {
        return new ReactiveTopicWorkerCoordinator(
                topicRegistry,
                messageRepository,
                lockExec,
                handlers,
                observer,
                repositoryFactory,
                superduperTaskScheduler,
                workerProperties.getClaimIntervalMs(),
                workerProperties.getClaimInitialDelayMs(),
                workerProperties.getShedlock().getLockAtMostForMs(),
                workerProperties.getShedlock().getLockAtLeastForMs());
    }

    SuperDuperWorkerReactiveService reactiveWorker(
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
                "superduper-claim-batch",
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
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        return new ReactiveOrphanReclaimer(
                maintenanceRepository,
                topicRegistry,
                repositoryFactory,
                observer,
                workerProperties.getOrphanTimeoutMs(),
                (int) workerProperties.getHeartbeatWindowMs());
    }

    ReactiveOrphanReclaimer reactiveOrphanReclaimer(
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
            ReactiveWorkerMessageRepository messageRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer) {
        return new ReactiveRedriveService(messageRepository, topicRegistry, repositoryFactory, observer);
    }

    ReactiveRedriveService reactiveRedriveService(
            ReactiveWorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new ReactiveRedriveService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnProperty(name = "superduper.worker.queue-health.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public ReactiveQueueHealthService reactiveQueueHealthService(
            ReactiveWorkerMessageRepository messageRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer) {
        return new ReactiveQueueHealthService(messageRepository, topicRegistry, repositoryFactory, observer);
    }

    ReactiveQueueHealthService reactiveQueueHealthService(
            ReactiveWorkerMessageRepository messageRepository, SuperduperObserver observer) {
        return new ReactiveQueueHealthService(messageRepository, observer);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnProperty(name = "superduper.worker.retention.enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public ReactiveCleanupService reactiveCleanupService(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            TopicRegistry topicRegistry,
            RepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            WorkerProperties workerProperties) {
        WorkerProperties.Retention retention = workerProperties.getRetention();
        return new ReactiveCleanupService(
                maintenanceRepository,
                topicRegistry,
                repositoryFactory,
                observer,
                retention.getProcessedRetentionDays(),
                retention.getStoppedRetentionDays(),
                retention.getHeartbeatRetentionDays());
    }

    ReactiveCleanupService reactiveCleanupService(
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
