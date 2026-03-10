package net.rsworld.superduper.worker.reactive;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReactiveOrphanReclaimer {
    private final ReactiveWorkerMaintenanceRepository maintenanceRepository;
    private final Map<String, ReactiveWorkerMaintenanceRepository> repositoriesByTopic;
    private final SuperduperObserver observer;
    private final TopicRegistryView topicRegistry;
    private final int orphanTimeoutSec;
    private final int heartbeatWindowSec;

    public ReactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
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

    public ReactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
            TopicRegistryView topicRegistry,
            SuperduperObserver observer,
            int orphanTimeoutMs,
            int heartbeatWindowMs) {
        this(maintenanceRepository, topicRegistry, null, observer, orphanTimeoutMs, heartbeatWindowMs);
    }

    public ReactiveOrphanReclaimer(
            ReactiveWorkerMaintenanceRepository maintenanceRepository,
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
        Flux<Integer> reclaims = topicRegistry == null
                ? Mono.zip(
                                maintenanceRepository.reclaimStaleProcessing(orphanTimeoutSec),
                                maintenanceRepository.reclaimMissingHeartbeats(heartbeatWindowSec))
                        .map(counts -> counts.getT1() + counts.getT2())
                        .flux()
                : Flux.fromIterable(topicRegistry.topics()).concatMap(topic -> {
                    ReactiveWorkerMaintenanceRepository topicRepository = repositoryFor(topic.kafkaTopic());
                    return Mono.zip(
                                    topicRepository.reclaimStaleProcessing(orphanTimeoutSec, topic.kafkaTopic()),
                                    topicRepository.reclaimMissingHeartbeats(heartbeatWindowSec, topic.kafkaTopic()))
                            .map(counts -> counts.getT1() + counts.getT2());
                });
        reclaims.reduce(0, Integer::sum)
                .doOnSuccess(reclaimedCount -> observer.maintenanceSucceeded(
                        new MaintenanceObservation("reactive", "all", "n/a", "orphan-reclaim", elapsedMs(started)),
                        reclaimedCount))
                .onErrorResume(e -> {
                    observer.maintenanceFailed(
                            new MaintenanceObservation("reactive", "all", "n/a", "orphan-reclaim", elapsedMs(started)),
                            e);
                    return Mono.empty();
                })
                .block();
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private ReactiveWorkerMaintenanceRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, maintenanceRepository);
    }

    private static Map<String, ReactiveWorkerMaintenanceRepository> buildRepositories(
            TopicRegistryView topicRegistry,
            ReactiveWorkerMaintenanceRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        if (topicRegistry == null) {
            return Map.of();
        }
        Map<String, ReactiveWorkerMaintenanceRepository> repositories = new LinkedHashMap<>();
        for (var topic : topicRegistry.topics()) {
            if (topic.table().isBlank()) {
                repositories.put(topic.kafkaTopic(), sharedRepository);
                continue;
            }
            if (repositoryFactory == null) {
                throw new IllegalStateException("TopicRepositoryFactory is required when topic '" + topic.kafkaTopic()
                        + "' uses dedicated table '" + topic.table() + "'");
            }
            repositories.put(topic.kafkaTopic(), repositoryFactory.createReactiveMaintenanceRepository(topic.table()));
        }
        return repositories;
    }
}
