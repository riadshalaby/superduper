package net.rsworld.superduper.worker.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.junit.jupiter.api.Test;

class TopicAwareWorkerServicesTest {

    @Test
    void queueHealthUsesSharedAndDedicatedRepositoriesPerTopic() {
        WorkerMessageRepository sharedRepository = mock(WorkerMessageRepository.class);
        WorkerMessageRepository dedicatedRepository = mock(WorkerMessageRepository.class);
        TopicRepositoryFactory repositoryFactory = mock(TopicRepositoryFactory.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(sharedRepository.countByStatus("orders.events")).thenReturn(Map.of("READY", 3L, "FAILED", 1L));
        when(dedicatedRepository.countByStatus("invoices.events")).thenReturn(Map.of("STOPPED", 2L));
        when(repositoryFactory.createWorkerRepository("invoice_messages")).thenReturn(dedicatedRepository);

        QueueHealthService service =
                new QueueHealthService(sharedRepository, topicRegistry(), repositoryFactory, observer, "worker-a");

        service.poll();

        verify(sharedRepository).countByStatus("orders.events");
        verify(dedicatedRepository).countByStatus("invoices.events");
        verify(observer)
                .queueBacklogObserved(
                        "blocking", "orders.events", Map.of("READY", 3L, "FAILED", 1L, "STOPPED", 0L, "PROCESSED", 0L));
        verify(observer)
                .queueBacklogObserved(
                        "blocking",
                        "invoices.events",
                        Map.of("READY", 0L, "FAILED", 0L, "STOPPED", 2L, "PROCESSED", 0L));
    }

    @Test
    void cleanupUsesDedicatedMaintenanceRepositoryWhenConfigured() {
        WorkerMaintenanceRepository sharedRepository = mock(WorkerMaintenanceRepository.class);
        WorkerMaintenanceRepository dedicatedRepository = mock(WorkerMaintenanceRepository.class);
        TopicRepositoryFactory repositoryFactory = mock(TopicRepositoryFactory.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repositoryFactory.createMaintenanceRepository("invoice_messages")).thenReturn(dedicatedRepository);
        when(sharedRepository.deleteProcessedOlderThan(14, "orders.events")).thenReturn(2);
        when(sharedRepository.deleteStoppedOlderThan(30, "orders.events")).thenReturn(1);
        when(dedicatedRepository.deleteProcessedOlderThan(14, "invoices.events"))
                .thenReturn(4);
        when(dedicatedRepository.deleteStoppedOlderThan(30, "invoices.events")).thenReturn(3);
        when(sharedRepository.deleteStaleHeartbeats(1)).thenReturn(5);

        CleanupService service =
                new CleanupService(sharedRepository, topicRegistry(), repositoryFactory, observer, 14, 30, 1);

        service.cleanup();

        verify(sharedRepository).deleteProcessedOlderThan(14, "orders.events");
        verify(sharedRepository).deleteStoppedOlderThan(30, "orders.events");
        verify(dedicatedRepository).deleteProcessedOlderThan(14, "invoices.events");
        verify(dedicatedRepository).deleteStoppedOlderThan(30, "invoices.events");
        verify(sharedRepository).deleteStaleHeartbeats(1);
        verify(observer).maintenanceCleanup(any(), eq(2));
        verify(observer).maintenanceCleanup(any(), eq(1));
        verify(observer).maintenanceCleanup(any(), eq(4));
        verify(observer).maintenanceCleanup(any(), eq(3));
        verify(observer).maintenanceCleanup(any(), eq(5));
    }

    @Test
    void orphanReclaimerUsesDedicatedMaintenanceRepositoryWhenConfigured() {
        WorkerMaintenanceRepository sharedRepository = mock(WorkerMaintenanceRepository.class);
        WorkerMaintenanceRepository dedicatedRepository = mock(WorkerMaintenanceRepository.class);
        TopicRepositoryFactory repositoryFactory = mock(TopicRepositoryFactory.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repositoryFactory.createMaintenanceRepository("invoice_messages")).thenReturn(dedicatedRepository);
        when(sharedRepository.reclaimStaleProcessing(120, "orders.events")).thenReturn(2);
        when(sharedRepository.reclaimMissingHeartbeats(90, "orders.events")).thenReturn(1);
        when(dedicatedRepository.reclaimStaleProcessing(120, "invoices.events")).thenReturn(4);
        when(dedicatedRepository.reclaimMissingHeartbeats(90, "invoices.events"))
                .thenReturn(3);

        OrphanReclaimer service =
                new OrphanReclaimer(sharedRepository, topicRegistry(), repositoryFactory, observer, 120_000, 90_000);

        service.reclaim();

        verify(sharedRepository).reclaimStaleProcessing(120, "orders.events");
        verify(sharedRepository).reclaimMissingHeartbeats(90, "orders.events");
        verify(dedicatedRepository).reclaimStaleProcessing(120, "invoices.events");
        verify(dedicatedRepository).reclaimMissingHeartbeats(90, "invoices.events");
        verify(observer).maintenanceSucceeded(any(), eq(10));
        verify(observer, never()).maintenanceFailed(any(), any());
    }

    @Test
    void redriveServiceUsesTopicAwareRepositoriesAndScansDedicatedOnRedriveOne() {
        WorkerMessageRepository sharedRepository = mock(WorkerMessageRepository.class);
        WorkerMessageRepository dedicatedRepository = mock(WorkerMessageRepository.class);
        TopicRepositoryFactory repositoryFactory = mock(TopicRepositoryFactory.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        ClaimedMessage row =
                new ClaimedMessage(11L, "invoices.events", "m-11", "k-11", "body", 2, "worker", "corr", "type");
        when(repositoryFactory.createWorkerRepository("invoice_messages")).thenReturn(dedicatedRepository);
        when(sharedRepository.findByStatus("FAILED", 10, "orders.events")).thenReturn(List.of(row));
        when(dedicatedRepository.redriveByStatus("STOPPED", 5, "invoices.events"))
                .thenReturn(2);
        when(sharedRepository.redriveById(11L)).thenReturn(0);
        when(dedicatedRepository.redriveById(11L)).thenReturn(1);

        RedriveService service =
                new RedriveService(sharedRepository, topicRegistry(), repositoryFactory, observer, "worker-a");

        assertThat(service.inspect("FAILED", 10, "orders.events")).containsExactly(row);
        assertThat(service.redriveBatch("STOPPED", 5, "invoices.events")).isEqualTo(2);
        assertThat(service.redriveOne(11L)).isTrue();

        verify(sharedRepository).findByStatus("FAILED", 10, "orders.events");
        verify(dedicatedRepository).redriveByStatus("STOPPED", 5, "invoices.events");
        verify(sharedRepository).redriveById(11L);
        verify(dedicatedRepository).redriveById(11L);
        verify(observer).workerRedriven(any(), eq(2));
        verify(observer).workerRedriven(any(), eq(1));
    }

    @Test
    void dedicatedTopicsRequireRepositoryFactoryForTopicAwareServices() {
        WorkerMessageRepository workerRepository = mock(WorkerMessageRepository.class);
        WorkerMaintenanceRepository maintenanceRepository = mock(WorkerMaintenanceRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        TopicRegistryView dedicatedOnly = topicRegistry(List.of(new TestTopicConfig(
                "invoices", "invoices.events", "invoicesHandler", 20, 7, "invoice_messages", "invoices-claim")));

        assertThatThrownBy(() -> new QueueHealthService(workerRepository, dedicatedOnly, null, observer, "worker-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TopicRepositoryFactory is required");
        assertThatThrownBy(() -> new CleanupService(maintenanceRepository, dedicatedOnly, null, observer, 14, 30, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TopicRepositoryFactory is required");
        assertThatThrownBy(() ->
                        new OrphanReclaimer(maintenanceRepository, dedicatedOnly, null, observer, 120_000, 90_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TopicRepositoryFactory is required");
        assertThatThrownBy(() -> new RedriveService(workerRepository, dedicatedOnly, null, observer, "worker-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TopicRepositoryFactory is required");
    }

    @Test
    void messageHandlingExceptionPreservesMessageAndCause() {
        RuntimeException cause = new RuntimeException("boom");

        MessageHandlingException withMessage = new MessageHandlingException("failed");
        MessageHandlingException withCause = new MessageHandlingException("failed", cause);

        assertThat(withMessage).hasMessage("failed").hasNoCause();
        assertThat(withCause).hasMessage("failed").hasCause(cause);
    }

    private static TopicRegistryView topicRegistry() {
        return topicRegistry(List.of(
                new TestTopicConfig("orders", "orders.events", "ordersHandler", 10, 5, "", "orders-claim"),
                new TestTopicConfig(
                        "invoices",
                        "invoices.events",
                        "invoicesHandler",
                        20,
                        7,
                        "invoice_messages",
                        "invoices-claim")));
    }

    private static TopicRegistryView topicRegistry(List<TestTopicConfig> topics) {
        return new TopicRegistryView() {
            @Override
            public List<TestTopicConfig> topics() {
                return topics;
            }

            @Override
            public List<String> kafkaTopics() {
                return topics.stream().map(TestTopicConfig::kafkaTopic).toList();
            }

            @Override
            public TopicConfigView getByKafkaTopic(String kafkaTopic) {
                return topics.stream()
                        .filter(topic -> topic.kafkaTopic().equals(kafkaTopic))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Missing topic " + kafkaTopic));
            }
        };
    }

    private record TestTopicConfig(
            String name,
            String kafkaTopic,
            String handlerBeanName,
            int batchSize,
            int maxRetries,
            String table,
            String claimLockName)
            implements TopicConfigView {}
}
