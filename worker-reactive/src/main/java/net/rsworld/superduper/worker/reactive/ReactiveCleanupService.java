package net.rsworld.superduper.worker.reactive;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveCleanupService {
    private static final String MODE_REACTIVE = "reactive";
    private static final String WORKER_ID = "n/a";

    private final ReactiveWorkerMaintenanceRepository repository;
    private final Map<String, ReactiveWorkerMaintenanceRepository> repositoriesByTopic;
    private final SuperduperObserver observer;
    private final TopicRegistryView topicRegistry;
    private final int processedRetentionDays;
    private final int stoppedRetentionDays;
    private final int heartbeatRetentionDays;

    public ReactiveCleanupService(
            ReactiveWorkerMaintenanceRepository repository,
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

    public ReactiveCleanupService(
            ReactiveWorkerMaintenanceRepository repository,
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

    public ReactiveCleanupService(
            ReactiveWorkerMaintenanceRepository repository,
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
        Mono<Void> topicCleanup = topicRegistry == null
                ? repository
                        .deleteProcessedOlderThan(processedRetentionDays)
                        .doOnError(error -> emitFailure(started, "cleanup-processed", error))
                        .flatMap(deletedProcessed -> repository
                                .deleteStoppedOlderThan(stoppedRetentionDays)
                                .doOnError(error -> emitFailure(started, "cleanup-stopped", error))
                                .doOnNext(deletedStopped ->
                                        emitCleanup(started, "default", deletedProcessed, deletedStopped)))
                        .then()
                : Flux.fromIterable(topicRegistry.topics())
                        .concatMap(topic -> {
                            ReactiveWorkerMaintenanceRepository topicRepository = repositoryFor(topic.kafkaTopic());
                            return topicRepository
                                    .deleteProcessedOlderThan(processedRetentionDays, topic.kafkaTopic())
                                    .doOnError(error -> emitFailure(started, "cleanup-processed", error))
                                    .flatMap(deletedProcessed -> topicRepository
                                            .deleteStoppedOlderThan(stoppedRetentionDays, topic.kafkaTopic())
                                            .doOnError(error -> emitFailure(started, "cleanup-stopped", error))
                                            .doOnNext(deletedStopped -> emitCleanup(
                                                    started, topic.kafkaTopic(), deletedProcessed, deletedStopped)));
                        })
                        .then();
        topicCleanup
                .then(Mono.defer(() -> repository
                        .deleteStaleHeartbeats(heartbeatRetentionDays)
                        .doOnError(error -> emitFailure(started, "cleanup-heartbeats", error))
                        .doOnNext(deletedHeartbeats -> observer.maintenanceCleanup(
                                new MaintenanceObservation(
                                        MODE_REACTIVE, "all", WORKER_ID, "cleanup-heartbeats", elapsedMs(started)),
                                deletedHeartbeats))))
                .onErrorResume(error -> Mono.empty())
                .block();
    }

    private void emitCleanup(long started, String topic, int deletedProcessed, int deletedStopped) {
        long durationMs = elapsedMs(started);
        observer.maintenanceCleanup(
                new MaintenanceObservation(MODE_REACTIVE, topic, WORKER_ID, "cleanup-processed", durationMs),
                deletedProcessed);
        observer.maintenanceCleanup(
                new MaintenanceObservation(MODE_REACTIVE, topic, WORKER_ID, "cleanup-stopped", durationMs),
                deletedStopped);
    }

    private void emitFailure(long started, String operation, Throwable error) {
        observer.maintenanceFailed(
                new MaintenanceObservation(MODE_REACTIVE, "all", WORKER_ID, operation, elapsedMs(started)), error);
    }

    private ReactiveWorkerMaintenanceRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, repository);
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

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
