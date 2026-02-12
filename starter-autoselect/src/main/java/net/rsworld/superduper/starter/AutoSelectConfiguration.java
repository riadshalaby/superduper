package net.rsworld.superduper.starter;

import javax.sql.DataSource;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.metrics.MetricsSuperduperObserver;
import net.rsworld.superduper.observability.logging.LoggingSuperduperObserver;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.worker.jdbc.HeartbeatService;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.OrphanReclaimer;
import net.rsworld.superduper.worker.jdbc.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveHeartbeatService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.ReactiveOrphanReclaimer;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(ObservabilityProperties.class)
public class AutoSelectConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SuperduperObserver superduperObserver(
            ObservabilityProperties properties, org.springframework.beans.factory.ObjectProvider<MeterRegistry> meterRegistry) {
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
    @ConditionalOnMissingBean
    public SuperDuperWorkerService jdbcWorker(
            WorkerMessageRepository messageRepository,
            org.springframework.transaction.PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            @Value("${superduper.worker.batch-size:100}") int batchSize,
            @Value("${superduper.worker.max-retries:5}") int maxRetries) {
        return new SuperDuperWorkerService(messageRepository, txm, lockExec, handler, observer, batchSize, maxRetries);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public HeartbeatService jdbcHeartbeatService(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") long heartbeatIntervalMs) {
        return new HeartbeatService(maintenanceRepository, observer, heartbeatIntervalMs);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
    @ConditionalOnMissingBean
    public OrphanReclaimer jdbcOrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") int heartbeatIntervalMs) {
        return new OrphanReclaimer(maintenanceRepository, observer, orphanTimeoutMs, heartbeatIntervalMs);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public SuperDuperWorkerReactiveService reactiveWorker(
            ReactiveWorkerMessageRepository messageRepository,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            @Value("${superduper.worker.batch-size:100}") int batchSize,
            @Value("${superduper.worker.max-retries:5}") int maxRetries,
            @Value("${superduper.worker.claim-interval-ms:5000}") long claimIntervalMs) {
        return new SuperDuperWorkerReactiveService(
                messageRepository, handler, observer, batchSize, maxRetries, claimIntervalMs);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public ReactiveHeartbeatService reactiveHeartbeatService(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") long heartbeatIntervalMs) {
        return new ReactiveHeartbeatService(maintenanceRepository, observer, heartbeatIntervalMs);
    }

    @Bean
    @ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
    @ConditionalOnMissingBean
    public ReactiveOrphanReclaimer reactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-interval-ms:30000}") int heartbeatIntervalMs) {
        return new ReactiveOrphanReclaimer(maintenanceRepository, observer, orphanTimeoutMs, heartbeatIntervalMs);
    }
}
