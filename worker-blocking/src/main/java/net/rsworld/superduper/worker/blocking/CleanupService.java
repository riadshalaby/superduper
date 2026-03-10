package net.rsworld.superduper.worker.blocking;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.scheduling.annotation.Scheduled;

public class CleanupService {
    private static final String MODE_BLOCKING = "blocking";
    private static final String WORKER_ID = "n/a";

    private final WorkerMaintenanceRepository repository;
    private final Map<String, WorkerMaintenanceRepository> repositoriesByTopic;
    private final SuperduperObserver observer;
    private final TopicRegistryView topicRegistry;
    private final int processedRetentionDays;
    private final int stoppedRetentionDays;
    private final int heartbeatRetentionDays;

    public CleanupService(
            WorkerMaintenanceRepository repository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            int processedRetentionDays,
            int stoppedRetentionDays,
            int heartbeatRetentionDays) {
        this.repository = repository;
        this.topicRegistry = topicRegistry;
        this.repositoriesByTopic = buildRepositories(topicRegistry, repository, repositoryFactory);
        this.observer = observer;
        this.processedRetentionDays = processedRetentionDays;
        this.stoppedRetentionDays = stoppedRetentionDays;
        this.heartbeatRetentionDays = heartbeatRetentionDays;
    }

    public CleanupService(
            WorkerMaintenanceRepository repository,
            TopicRegistryView topicRegistry,
            SuperduperObserver observer,
            int processedRetentionDays,
            int stoppedRetentionDays,
            int heartbeatRetentionDays) {
        this(
                repository,
                topicRegistry,
                null,
                observer,
                processedRetentionDays,
                stoppedRetentionDays,
                heartbeatRetentionDays);
    }

    public CleanupService(
            WorkerMaintenanceRepository repository,
            SuperduperObserver observer,
            int processedRetentionDays,
            int stoppedRetentionDays,
            int heartbeatRetentionDays) {
        this(repository, null, null, observer, processedRetentionDays, stoppedRetentionDays, heartbeatRetentionDays);
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.retention.interval-ms:86400000}",
            initialDelayString = "${superduper.worker.retention.initial-delay-ms:60000}")
    public void cleanup() {
        long started = System.nanoTime();
        String operation = "cleanup-processed";
        try {
            if (topicRegistry == null) {
                int deletedProcessed = repository.deleteProcessedOlderThan(processedRetentionDays);
                operation = "cleanup-stopped";
                int deletedStopped = repository.deleteStoppedOlderThan(stoppedRetentionDays);
                long durationMs = elapsedMs(started);
                observer.maintenanceCleanup(
                        new MaintenanceObservation(
                                MODE_BLOCKING, "default", WORKER_ID, "cleanup-processed", durationMs),
                        deletedProcessed);
                observer.maintenanceCleanup(
                        new MaintenanceObservation(MODE_BLOCKING, "default", WORKER_ID, "cleanup-stopped", durationMs),
                        deletedStopped);
            } else {
                for (var topic : topicRegistry.topics()) {
                    WorkerMaintenanceRepository topicRepository = repositoryFor(topic.kafkaTopic());
                    int deletedProcessed =
                            topicRepository.deleteProcessedOlderThan(processedRetentionDays, topic.kafkaTopic());
                    operation = "cleanup-stopped";
                    int deletedStopped =
                            topicRepository.deleteStoppedOlderThan(stoppedRetentionDays, topic.kafkaTopic());
                    long durationMs = elapsedMs(started);
                    observer.maintenanceCleanup(
                            new MaintenanceObservation(
                                    MODE_BLOCKING, topic.kafkaTopic(), WORKER_ID, "cleanup-processed", durationMs),
                            deletedProcessed);
                    observer.maintenanceCleanup(
                            new MaintenanceObservation(
                                    MODE_BLOCKING, topic.kafkaTopic(), WORKER_ID, "cleanup-stopped", durationMs),
                            deletedStopped);
                    operation = "cleanup-processed";
                }
            }
            operation = "cleanup-heartbeats";
            int deletedHeartbeats = repository.deleteStaleHeartbeats(heartbeatRetentionDays);
            long durationMs = elapsedMs(started);
            observer.maintenanceCleanup(
                    new MaintenanceObservation(MODE_BLOCKING, "all", WORKER_ID, "cleanup-heartbeats", durationMs),
                    deletedHeartbeats);
        } catch (RuntimeException error) {
            observer.maintenanceFailed(
                    new MaintenanceObservation(MODE_BLOCKING, "all", WORKER_ID, operation, elapsedMs(started)), error);
            throw error;
        }
    }

    private WorkerMaintenanceRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, repository);
    }

    private static Map<String, WorkerMaintenanceRepository> buildRepositories(
            TopicRegistryView topicRegistry,
            WorkerMaintenanceRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        if (topicRegistry == null) {
            return Map.of();
        }
        Map<String, WorkerMaintenanceRepository> repositories = new LinkedHashMap<>();
        for (var topic : topicRegistry.topics()) {
            if (topic.table().isBlank()) {
                repositories.put(topic.kafkaTopic(), sharedRepository);
                continue;
            }
            if (repositoryFactory == null) {
                throw new IllegalStateException("TopicRepositoryFactory is required when topic '" + topic.kafkaTopic()
                        + "' uses dedicated table '" + topic.table() + "'");
            }
            repositories.put(topic.kafkaTopic(), repositoryFactory.createMaintenanceRepository(topic.table()));
        }
        return repositories;
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
