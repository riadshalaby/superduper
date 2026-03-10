package net.rsworld.superduper.worker.blocking;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OrphanReclaimer {
    private final WorkerMaintenanceRepository maintenanceRepository;
    private final Map<String, WorkerMaintenanceRepository> repositoriesByTopic;
    private final SuperduperObserver observer;
    private final TopicRegistryView topicRegistry;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public OrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
            @Value("${superduper.worker.heartbeat-window-ms:90000}") int heartbeatWindowMs) {
        this.maintenanceRepository = maintenanceRepository;
        this.topicRegistry = topicRegistry;
        this.repositoriesByTopic = buildRepositories(topicRegistry, maintenanceRepository, repositoryFactory);
        this.observer = observer;
        this.orphanTimeoutSec = orphanTimeoutMs / 1000;
        this.heartbeatWindowSec = heartbeatWindowMs / 1000;
    }

    public OrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            TopicRegistryView topicRegistry,
            SuperduperObserver observer,
            int orphanTimeoutMs,
            int heartbeatWindowMs) {
        this(maintenanceRepository, topicRegistry, null, observer, orphanTimeoutMs, heartbeatWindowMs);
    }

    public OrphanReclaimer(
            WorkerMaintenanceRepository maintenanceRepository,
            SuperduperObserver observer,
            int orphanTimeoutMs,
            int heartbeatWindowMs) {
        this(maintenanceRepository, null, null, observer, orphanTimeoutMs, heartbeatWindowMs);
    }

    @Scheduled(
            fixedDelayString = "${superduper.worker.orphan-timeout-ms:120000}",
            initialDelayString = "${superduper.worker.orphan-initial-delay-ms:15000}")
    public void reclaim() {
        long started = System.nanoTime();
        try {
            int reclaimedCount = 0;
            if (topicRegistry == null) {
                reclaimedCount += maintenanceRepository.reclaimStaleProcessing(orphanTimeoutSec);
                reclaimedCount += maintenanceRepository.reclaimMissingHeartbeats(heartbeatWindowSec);
            } else {
                for (var topic : topicRegistry.topics()) {
                    WorkerMaintenanceRepository topicRepository = repositoryFor(topic.kafkaTopic());
                    reclaimedCount += topicRepository.reclaimStaleProcessing(orphanTimeoutSec, topic.kafkaTopic());
                    reclaimedCount += topicRepository.reclaimMissingHeartbeats(heartbeatWindowSec, topic.kafkaTopic());
                }
            }
            observer.maintenanceSucceeded(
                    new MaintenanceObservation("blocking", "all", "n/a", "orphan-reclaim", elapsedMs(started)),
                    reclaimedCount);
        } catch (RuntimeException e) {
            observer.maintenanceFailed(
                    new MaintenanceObservation("blocking", "all", "n/a", "orphan-reclaim", elapsedMs(started)), e);
            throw e;
        }
    }

    private WorkerMaintenanceRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, maintenanceRepository);
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
