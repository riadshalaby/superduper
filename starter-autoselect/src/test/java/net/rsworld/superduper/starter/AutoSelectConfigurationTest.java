package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
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
import net.rsworld.superduper.worker.blocking.HeartbeatService;
import net.rsworld.superduper.worker.blocking.MessageHandler;
import net.rsworld.superduper.worker.blocking.OrphanReclaimer;
import net.rsworld.superduper.worker.blocking.QueueHealthService;
import net.rsworld.superduper.worker.blocking.RedriveService;
import net.rsworld.superduper.worker.blocking.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveHeartbeatService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.ReactiveOrphanReclaimer;
import net.rsworld.superduper.worker.reactive.ReactiveQueueHealthService;
import net.rsworld.superduper.worker.reactive.ReactiveRedriveService;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;

class AutoSelectConfigurationTest {
    private static final String SCHEDULE_DELAY_PROPERTY = "3600000";

    private static WorkerProperties workerProperties() {
        WorkerProperties p = new WorkerProperties();
        p.setBatchSize(100);
        p.setMaxRetries(5);
        p.setHeartbeatIntervalMs(30_000);
        p.setHeartbeatWindowMs(90_000);
        p.setOrphanTimeoutMs(120_000);
        return p;
    }

    private static ApplicationContextRunner jdbcContextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AutoSelectConfiguration.class))
                .withPropertyValues(
                        "superduper.consumer.type=spring",
                        "superduper.worker.claim-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.heartbeat-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.orphan-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY)
                .withBean(LockProvider.class, () -> mock(LockProvider.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(WorkerMessageRepository.class, () -> {
                    WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
                    when(repository.countByStatus()).thenReturn(Map.of());
                    return repository;
                })
                .withBean(WorkerMaintenanceRepository.class, () -> mock(WorkerMaintenanceRepository.class))
                .withBean(
                        MessageHandler.class,
                        () -> row -> net.rsworld.superduper.worker.blocking.ProcessingResult.SUCCESS);
    }

    private static ApplicationContextRunner reactiveContextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AutoSelectConfiguration.class))
                .withPropertyValues(
                        "superduper.consumer.type=reactor",
                        "superduper.worker.claim-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.heartbeat-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.orphan-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY)
                .withBean(LockProvider.class, () -> mock(LockProvider.class))
                .withBean(ReactiveWorkerMessageRepository.class, () -> {
                    ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
                    when(repository.countByStatus()).thenReturn(reactor.core.publisher.Mono.just(Map.of()));
                    return repository;
                })
                .withBean(
                        ReactiveWorkerMaintenanceRepository.class,
                        () -> mock(ReactiveWorkerMaintenanceRepository.class))
                .withBean(
                        ReactiveMessageHandler.class,
                        () -> row -> reactor.core.publisher.Mono.just(
                                net.rsworld.superduper.worker.reactive.ProcessingResult.SUCCESS));
    }

    @Test
    void createsJdbcBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ConsumerMetadataResolver metadataResolver = cfg.consumerMetadataResolver();
        DataSource ds = mock(DataSource.class);
        WorkerProperties workerProperties = workerProperties();
        LockProvider lp = cfg.lockProvider(ds, workerProperties);
        LockingTaskExecutor exec = cfg.lockingTaskExecutor(lp);

        WorkerMessageRepository mr = mock(WorkerMessageRepository.class);
        WorkerMaintenanceRepository maintenanceRepository = mock(WorkerMaintenanceRepository.class);
        PlatformTransactionManager txm = mock(PlatformTransactionManager.class);
        MessageHandler handler = row -> net.rsworld.superduper.worker.blocking.ProcessingResult.SUCCESS;
        var observer = NoopSuperduperObserver.INSTANCE;

        SuperDuperWorkerService svc = cfg.jdbcWorker(mr, txm, exec, handler, observer, workerProperties);
        HeartbeatService hb = cfg.jdbcHeartbeatService(maintenanceRepository, observer);
        OrphanReclaimer rec = cfg.jdbcOrphanReclaimer(maintenanceRepository, observer, workerProperties);
        RedriveService redriveService = cfg.jdbcRedriveService(mr, observer);
        QueueHealthService queueHealthService = cfg.jdbcQueueHealthService(mr, observer);

        assertThat(lp).isNotNull();
        assertThat(exec).isNotNull();
        assertThat(metadataResolver).isInstanceOf(DefaultConsumerMetadataResolver.class);
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();
        assertThat(redriveService).isNotNull();
        assertThat(queueHealthService).isNotNull();

        when(maintenanceRepository.reclaimStaleProcessing(anyInt())).thenReturn(1);
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt())).thenReturn(2);
        when(mr.countByStatus()).thenReturn(java.util.Map.of("READY", 1L));
        hb.heartbeat();
        rec.reclaim();
        queueHealthService.poll();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
        verify(mr).countByStatus();
    }

    @Test
    void createsReactiveBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        ReactiveWorkerMaintenanceRepository maintenanceRepository = mock(ReactiveWorkerMaintenanceRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        WorkerProperties workerProperties = workerProperties();
        ReactiveMessageHandler h = row ->
                reactor.core.publisher.Mono.just(net.rsworld.superduper.worker.reactive.ProcessingResult.SUCCESS);
        var observer = NoopSuperduperObserver.INSTANCE;
        SuperDuperWorkerReactiveService svc = cfg.reactiveWorker(mr, lockExec, h, observer, workerProperties);
        ReactiveHeartbeatService hb = cfg.reactiveHeartbeatService(maintenanceRepository, observer);
        ReactiveOrphanReclaimer rec = cfg.reactiveOrphanReclaimer(maintenanceRepository, observer, workerProperties);
        ReactiveRedriveService redriveService = cfg.reactiveRedriveService(mr, observer);
        ReactiveQueueHealthService queueHealthService = cfg.reactiveQueueHealthService(mr, observer);
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();
        assertThat(redriveService).isNotNull();
        assertThat(queueHealthService).isNotNull();

        when(maintenanceRepository.heartbeat(anyString())).thenReturn(reactor.core.publisher.Mono.empty());
        when(maintenanceRepository.reclaimStaleProcessing(anyInt())).thenReturn(reactor.core.publisher.Mono.just(1));
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt())).thenReturn(reactor.core.publisher.Mono.just(2));
        when(mr.countByStatus()).thenReturn(reactor.core.publisher.Mono.just(java.util.Map.of("READY", 1L)));
        hb.heartbeat();
        rec.reclaim();
        queueHealthService.poll();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
        verify(mr).countByStatus();
    }

    @Test
    void queueHealthBeansAreDisabledByDefault() {
        jdbcContextRunner().run(context -> assertThat(context).doesNotHaveBean(QueueHealthService.class));
        reactiveContextRunner().run(context -> assertThat(context).doesNotHaveBean(ReactiveQueueHealthService.class));
    }

    @Test
    void queueHealthBeansCanBeEnabledPerStack() {
        jdbcContextRunner()
                .withPropertyValues(
                        "superduper.worker.queue-health.enabled=true",
                        "superduper.worker.queue-health.interval-ms=15000")
                .run(context -> assertThat(context).hasSingleBean(QueueHealthService.class));

        reactiveContextRunner()
                .withPropertyValues(
                        "superduper.worker.queue-health.enabled=true",
                        "superduper.worker.queue-health.interval-ms=15000")
                .run(context -> assertThat(context).hasSingleBean(ReactiveQueueHealthService.class));
    }

    @Test
    void observer_isNoop_whenObservabilityDisabled() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.setEnabled(false);
        ObjectProvider<MeterRegistry> meterRegistry = mock(ObjectProvider.class);

        SuperduperObserver observer = cfg.superduperObserver(properties, meterRegistry);

        assertThat(observer).isSameAs(NoopSuperduperObserver.INSTANCE);
        verify(meterRegistry, never()).getIfAvailable();
    }

    @Test
    void observer_usesMetrics_whenMetricsEnabledAndRegistryPresent() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.getOutputs().getMetrics().setEnabled(true);
        ObjectProvider<MeterRegistry> meterRegistry = mock(ObjectProvider.class);
        when(meterRegistry.getIfAvailable()).thenReturn(new SimpleMeterRegistry());

        SuperduperObserver observer = cfg.superduperObserver(properties, meterRegistry);

        assertThat(observer).isInstanceOf(MetricsSuperduperObserver.class);
    }

    @Test
    void observer_fallsBackToLogging_whenMetricsEnabledButNoRegistry() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.getOutputs().getMetrics().setEnabled(true);
        ObjectProvider<MeterRegistry> meterRegistry = mock(ObjectProvider.class);
        when(meterRegistry.getIfAvailable()).thenReturn(null);

        SuperduperObserver observer = cfg.superduperObserver(properties, meterRegistry);

        assertThat(observer).isInstanceOf(LoggingSuperduperObserver.class);
    }

    @Test
    void observer_isNoop_whenBothOutputsDisabled() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.getOutputs().getLog().setEnabled(false);
        properties.getOutputs().getMetrics().setEnabled(false);
        ObjectProvider<MeterRegistry> meterRegistry = mock(ObjectProvider.class);

        SuperduperObserver observer = cfg.superduperObserver(properties, meterRegistry);

        assertThat(observer).isSameAs(NoopSuperduperObserver.INSTANCE);
    }
}
