package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import net.rsworld.superduper.outbox.blocking.OutboxService;
import net.rsworld.superduper.outbox.reactive.ReactiveOutboxService;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;
import reactor.core.publisher.Mono;

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
                        "superduper.kafka.topic=test-topic",
                        "superduper.worker.claim-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.heartbeat-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.orphan-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY)
                .withBean(LockProvider.class, () -> mock(LockProvider.class))
                .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean(WorkerMessageRepository.class, () -> {
                    WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
                    when(repository.countByStatus()).thenReturn(Map.of());
                    when(repository.countByStatus(anyString())).thenReturn(Map.of());
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
                        "superduper.kafka.topic=test-topic",
                        "superduper.worker.claim-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.heartbeat-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY,
                        "superduper.worker.orphan-initial-delay-ms=" + SCHEDULE_DELAY_PROPERTY)
                .withBean(LockProvider.class, () -> mock(LockProvider.class))
                .withBean(ReactiveWorkerMessageRepository.class, () -> {
                    ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
                    when(repository.countByStatus()).thenReturn(reactor.core.publisher.Mono.just(Map.of()));
                    when(repository.countByStatus(anyString())).thenReturn(reactor.core.publisher.Mono.just(Map.of()));
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

    private static ApplicationContextRunner jdbcOutboxContextRunner() {
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
                    when(repository.countByStatus(anyString())).thenReturn(Map.of());
                    return repository;
                })
                .withBean(WorkerMaintenanceRepository.class, () -> mock(WorkerMaintenanceRepository.class))
                .withBean(
                        "sharedOutboxHandler",
                        MessageHandler.class,
                        () -> row -> net.rsworld.superduper.worker.blocking.ProcessingResult.SUCCESS)
                .withBean(
                        "dedicatedOutboxHandler",
                        MessageHandler.class,
                        () -> row -> net.rsworld.superduper.worker.blocking.ProcessingResult.SUCCESS);
    }

    private static ApplicationContextRunner reactiveOutboxContextRunner() {
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
                    when(repository.countByStatus(anyString())).thenReturn(reactor.core.publisher.Mono.just(Map.of()));
                    return repository;
                })
                .withBean(
                        ReactiveWorkerMaintenanceRepository.class,
                        () -> mock(ReactiveWorkerMaintenanceRepository.class))
                .withBean(
                        "sharedOutboxHandler",
                        ReactiveMessageHandler.class,
                        () -> row -> reactor.core.publisher.Mono.just(
                                net.rsworld.superduper.worker.reactive.ProcessingResult.SUCCESS))
                .withBean(
                        "dedicatedOutboxHandler",
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
        CleanupService cleanupService = cfg.jdbcCleanupService(maintenanceRepository, observer, workerProperties);

        assertThat(lp).isNotNull();
        assertThat(exec).isNotNull();
        assertThat(metadataResolver).isInstanceOf(DefaultConsumerMetadataResolver.class);
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();
        assertThat(redriveService).isNotNull();
        assertThat(queueHealthService).isNotNull();
        assertThat(cleanupService).isNotNull();

        when(maintenanceRepository.reclaimStaleProcessing(anyInt())).thenReturn(1);
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt())).thenReturn(2);
        when(maintenanceRepository.deleteProcessedOlderThan(anyInt())).thenReturn(3);
        when(maintenanceRepository.deleteStoppedOlderThan(anyInt())).thenReturn(4);
        when(maintenanceRepository.deleteStaleHeartbeats(anyInt())).thenReturn(5);
        when(mr.countByStatus()).thenReturn(java.util.Map.of("READY", 1L));
        hb.heartbeat();
        rec.reclaim();
        queueHealthService.poll();
        cleanupService.cleanup();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
        verify(maintenanceRepository).deleteProcessedOlderThan(anyInt());
        verify(maintenanceRepository).deleteStoppedOlderThan(anyInt());
        verify(maintenanceRepository).deleteStaleHeartbeats(anyInt());
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
        ReactiveCleanupService cleanupService =
                cfg.reactiveCleanupService(maintenanceRepository, observer, workerProperties);
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();
        assertThat(redriveService).isNotNull();
        assertThat(queueHealthService).isNotNull();
        assertThat(cleanupService).isNotNull();

        when(maintenanceRepository.heartbeat(anyString())).thenReturn(reactor.core.publisher.Mono.empty());
        when(maintenanceRepository.reclaimStaleProcessing(anyInt())).thenReturn(reactor.core.publisher.Mono.just(1));
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt())).thenReturn(reactor.core.publisher.Mono.just(2));
        when(maintenanceRepository.deleteProcessedOlderThan(anyInt())).thenReturn(reactor.core.publisher.Mono.just(3));
        when(maintenanceRepository.deleteStoppedOlderThan(anyInt())).thenReturn(reactor.core.publisher.Mono.just(4));
        when(maintenanceRepository.deleteStaleHeartbeats(anyInt())).thenReturn(reactor.core.publisher.Mono.just(5));
        when(mr.countByStatus()).thenReturn(reactor.core.publisher.Mono.just(java.util.Map.of("READY", 1L)));
        hb.heartbeat();
        rec.reclaim();
        queueHealthService.poll();
        cleanupService.cleanup();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
        verify(maintenanceRepository).deleteProcessedOlderThan(anyInt());
        verify(maintenanceRepository).deleteStoppedOlderThan(anyInt());
        verify(maintenanceRepository).deleteStaleHeartbeats(anyInt());
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
    void cleanupBeansAreDisabledByDefault() {
        jdbcContextRunner().run(context -> assertThat(context).doesNotHaveBean(CleanupService.class));
        reactiveContextRunner().run(context -> assertThat(context).doesNotHaveBean(ReactiveCleanupService.class));
    }

    @Test
    void cleanupBeansCanBeEnabledPerStack() {
        jdbcContextRunner()
                .withPropertyValues(
                        "superduper.worker.retention.enabled=true", "superduper.worker.retention.interval-ms=86400000")
                .run(context -> assertThat(context).hasSingleBean(CleanupService.class));

        reactiveContextRunner()
                .withPropertyValues(
                        "superduper.worker.retention.enabled=true", "superduper.worker.retention.interval-ms=86400000")
                .run(context -> assertThat(context).hasSingleBean(ReactiveCleanupService.class));
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

    @Test
    void topicRegistryMergesOutboxesWithoutSubscribingKafkaToOutboxNames() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        TopicProperties topicProperties = new TopicProperties();
        TopicProperties.TopicConfig topicConfig = new TopicProperties.TopicConfig();
        topicConfig.setKafkaTopic("orders.events");
        topicConfig.setHandler("ordersHandler");
        topicProperties.setTopics(Map.of("orders", topicConfig));

        OutboxProperties outboxProperties = new OutboxProperties();
        OutboxProperties.OutboxConfig sharedOutbox = new OutboxProperties.OutboxConfig();
        sharedOutbox.setHandler("sharedOutboxHandler");
        OutboxProperties.OutboxConfig dedicatedOutbox = new OutboxProperties.OutboxConfig();
        dedicatedOutbox.setHandler("dedicatedOutboxHandler");
        dedicatedOutbox.setBatchSize(25);
        dedicatedOutbox.setMaxRetries(8);
        dedicatedOutbox.setTable("outbox_messages");
        outboxProperties.setOutbox(Map.of("shared-outbox", sharedOutbox, "dedicated-outbox", dedicatedOutbox));

        TopicRegistry topicRegistry = cfg.topicRegistry(
                topicProperties,
                outboxProperties,
                workerProperties(),
                "",
                "spring",
                Map.of("ordersHandler", row -> net.rsworld.superduper.worker.blocking.ProcessingResult.SUCCESS),
                Map.of());

        assertThat(topicRegistry.kafkaTopics()).containsExactly("orders.events");
        assertThat(topicRegistry.topics())
                .extracting(TopicRegistry.ResolvedTopicConfig::name)
                .containsExactlyInAnyOrder("orders", "shared-outbox", "dedicated-outbox");
        assertThat(topicRegistry.topics())
                .filteredOn(topic -> topic.name().equals("shared-outbox"))
                .singleElement()
                .satisfies(topic -> {
                    assertThat(topic.kafkaTopic()).isEmpty();
                    assertThat(topic.handlerBeanName()).isEqualTo("sharedOutboxHandler");
                    assertThat(topic.batchSize()).isEqualTo(workerProperties().getBatchSize());
                    assertThat(topic.maxRetries()).isEqualTo(workerProperties().getMaxRetries());
                    assertThat(topic.claimLockName()).isEqualTo("superduper-claim-shared-outbox");
                });
        assertThat(topicRegistry.topics())
                .filteredOn(topic -> topic.name().equals("dedicated-outbox"))
                .singleElement()
                .satisfies(topic -> {
                    assertThat(topic.kafkaTopic()).isEmpty();
                    assertThat(topic.handlerBeanName()).isEqualTo("dedicatedOutboxHandler");
                    assertThat(topic.batchSize()).isEqualTo(25);
                    assertThat(topic.maxRetries()).isEqualTo(8);
                    assertThat(topic.table()).isEqualTo("outbox_messages");
                    assertThat(topic.claimLockName()).isEqualTo("superduper-claim-dedicated-outbox");
                });
    }

    @Test
    void topicRegistryAllowsOutboxOnlyConfiguration() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        OutboxProperties outboxProperties = new OutboxProperties();
        OutboxProperties.OutboxConfig outboxConfig = new OutboxProperties.OutboxConfig();
        outboxConfig.setHandler("ordersOutboxHandler");
        outboxProperties.setOutbox(Map.of("orders-outbox", outboxConfig));

        TopicRegistry topicRegistry = cfg.topicRegistry(
                new TopicProperties(), outboxProperties, workerProperties(), "", "spring", Map.of(), Map.of());

        assertThat(topicRegistry.kafkaTopics()).isEmpty();
        assertThat(topicRegistry.topics()).singleElement().satisfies(topic -> assertThat(topic.name())
                .isEqualTo("orders-outbox"));
    }

    @Test
    void topicRegistryRequiresOutboxHandler() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        OutboxProperties outboxProperties = new OutboxProperties();
        outboxProperties.setOutbox(Map.of("orders-outbox", new OutboxProperties.OutboxConfig()));

        assertThatThrownBy(() -> cfg.topicRegistry(
                        new TopicProperties(), outboxProperties, workerProperties(), "", "spring", Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("superduper.outbox.orders-outbox.handler must be set");
    }

    @Test
    void createsBlockingOutboxServiceOnlyWhenOutboxesConfigured() {
        MessageIngestRepository sharedRepository = mock(MessageIngestRepository.class);
        MessageIngestRepository dedicatedRepository = mock(MessageIngestRepository.class);
        RepositoryFactory repositoryFactory = mock(RepositoryFactory.class);
        when(repositoryFactory.createIngestRepository("dedicated_outbox_messages"))
                .thenReturn(dedicatedRepository);

        jdbcOutboxContextRunner()
                .withPropertyValues(
                        "superduper.outbox.shared-outbox.handler=sharedOutboxHandler",
                        "superduper.outbox.dedicated-outbox.handler=dedicatedOutboxHandler",
                        "superduper.outbox.dedicated-outbox.table=dedicated_outbox_messages")
                .withBean(MessageIngestRepository.class, () -> sharedRepository)
                .withBean(RepositoryFactory.class, () -> repositoryFactory)
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxService.class);
                    OutboxService outboxService = context.getBean(OutboxService.class);

                    outboxService.send("shared-outbox", "shared-key", "{\"event\":\"shared\"}");
                    outboxService.send("dedicated-outbox", "dedicated-key", "{\"event\":\"dedicated\"}");

                    verify(sharedRepository)
                            .upsertReadyMessage(
                                    eq("shared-outbox"),
                                    anyString(),
                                    eq("shared-key"),
                                    eq("{\"event\":\"shared\"}"),
                                    any(),
                                    isNull(),
                                    isNull());
                    verify(dedicatedRepository)
                            .upsertReadyMessage(
                                    eq("dedicated-outbox"),
                                    anyString(),
                                    eq("dedicated-key"),
                                    eq("{\"event\":\"dedicated\"}"),
                                    any(),
                                    isNull(),
                                    isNull());
                    verify(repositoryFactory).createIngestRepository("dedicated_outbox_messages");
                });

        jdbcContextRunner()
                .withBean(MessageIngestRepository.class, () -> mock(MessageIngestRepository.class))
                .run(context -> assertThat(context).doesNotHaveBean(OutboxService.class));
    }

    @Test
    void createsReactiveOutboxServiceOnlyWhenOutboxesConfigured() {
        ReactiveMessageIngestRepository sharedRepository = mock(ReactiveMessageIngestRepository.class);
        ReactiveMessageIngestRepository dedicatedRepository = mock(ReactiveMessageIngestRepository.class);
        RepositoryFactory repositoryFactory = mock(RepositoryFactory.class);
        when(sharedRepository.upsertReadyMessage(
                        anyString(), anyString(), anyString(), anyString(), any(), any(), any()))
                .thenReturn(Mono.empty());
        when(dedicatedRepository.upsertReadyMessage(
                        anyString(), anyString(), anyString(), anyString(), any(), any(), any()))
                .thenReturn(Mono.empty());
        when(repositoryFactory.createReactiveIngestRepository("dedicated_outbox_messages"))
                .thenReturn(dedicatedRepository);

        reactiveOutboxContextRunner()
                .withPropertyValues(
                        "superduper.outbox.shared-outbox.handler=sharedOutboxHandler",
                        "superduper.outbox.dedicated-outbox.handler=dedicatedOutboxHandler",
                        "superduper.outbox.dedicated-outbox.table=dedicated_outbox_messages")
                .withBean(ReactiveMessageIngestRepository.class, () -> sharedRepository)
                .withBean(RepositoryFactory.class, () -> repositoryFactory)
                .run(context -> {
                    assertThat(context).hasSingleBean(ReactiveOutboxService.class);
                    ReactiveOutboxService outboxService = context.getBean(ReactiveOutboxService.class);

                    outboxService
                            .send("shared-outbox", "shared-key", "{\"event\":\"shared\"}")
                            .block();
                    outboxService
                            .send("dedicated-outbox", "dedicated-key", "{\"event\":\"dedicated\"}")
                            .block();

                    verify(sharedRepository)
                            .upsertReadyMessage(
                                    eq("shared-outbox"),
                                    anyString(),
                                    eq("shared-key"),
                                    eq("{\"event\":\"shared\"}"),
                                    any(),
                                    isNull(),
                                    isNull());
                    verify(dedicatedRepository)
                            .upsertReadyMessage(
                                    eq("dedicated-outbox"),
                                    anyString(),
                                    eq("dedicated-key"),
                                    eq("{\"event\":\"dedicated\"}"),
                                    any(),
                                    isNull(),
                                    isNull());
                    verify(repositoryFactory).createReactiveIngestRepository("dedicated_outbox_messages");
                });

        reactiveContextRunner()
                .withBean(ReactiveMessageIngestRepository.class, () -> mock(ReactiveMessageIngestRepository.class))
                .run(context -> assertThat(context).doesNotHaveBean(ReactiveOutboxService.class));
    }
}
