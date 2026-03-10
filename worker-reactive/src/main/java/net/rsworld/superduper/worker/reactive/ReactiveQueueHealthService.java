package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Flux;

/** Publishes periodic queue backlog snapshots for metrics backends. */
public class ReactiveQueueHealthService {
    private static final String MODE_REACTIVE = "reactive";

    private final ReactiveWorkerMessageRepository repository;
    private final Map<String, ReactiveWorkerMessageRepository> repositoriesByTopic;
    private final SuperduperObserver observer;
    private final TopicRegistryView topicRegistry;

    /**
     * Creates a queue health service using the current runtime id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     */
    public ReactiveQueueHealthService(
            ReactiveWorkerMessageRepository repository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer) {
        this(
                repository,
                topicRegistry,
                repositoryFactory,
                observer,
                ManagementFactory.getRuntimeMXBean().getName());
    }

    public ReactiveQueueHealthService(
            ReactiveWorkerMessageRepository repository, TopicRegistryView topicRegistry, SuperduperObserver observer) {
        this(repository, topicRegistry, null, observer);
    }

    public ReactiveQueueHealthService(ReactiveWorkerMessageRepository repository, SuperduperObserver observer) {
        this(repository, null, observer, ManagementFactory.getRuntimeMXBean().getName());
    }

    /**
     * Creates a queue health service.
     *
     * @param repository the message repository
     * @param observer the observability sink
     * @param ignoredWorkerId retained for constructor symmetry with other services
     */
    public ReactiveQueueHealthService(
            ReactiveWorkerMessageRepository repository, SuperduperObserver observer, String ignoredWorkerId) {
        this(repository, null, null, observer, ignoredWorkerId);
    }

    public ReactiveQueueHealthService(
            ReactiveWorkerMessageRepository repository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            String ignoredWorkerId) {
        this.repository = repository;
        this.topicRegistry = topicRegistry;
        this.repositoriesByTopic = buildRepositories(topicRegistry, repository, repositoryFactory);
        this.observer = observer;
    }

    public ReactiveQueueHealthService(
            ReactiveWorkerMessageRepository repository,
            TopicRegistryView topicRegistry,
            SuperduperObserver observer,
            String ignoredWorkerId) {
        this(repository, topicRegistry, null, observer, ignoredWorkerId);
    }

    /** Polls current queue counts and publishes them to observability backends. */
    @Scheduled(
            fixedDelayString =
                    "${superduper.worker.queue-health.interval-ms:${superduper.worker.queue-health-interval-ms:60000}}")
    public void poll() {
        if (topicRegistry == null) {
            repository
                    .countByStatus()
                    .map(ReactiveQueueHealthService::normalizedCounts)
                    .doOnNext(counts -> observer.queueBacklogObserved(MODE_REACTIVE, counts))
                    .block();
            return;
        }
        Flux.fromIterable(topicRegistry.topics())
                .concatMap(topic -> repositoryFor(topic.kafkaTopic())
                        .countByStatus(topic.kafkaTopic())
                        .map(ReactiveQueueHealthService::normalizedCounts)
                        .doOnNext(counts -> observer.queueBacklogObserved(MODE_REACTIVE, topic.kafkaTopic(), counts)))
                .then()
                .block();
    }

    private ReactiveWorkerMessageRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, repository);
    }

    private static Map<String, ReactiveWorkerMessageRepository> buildRepositories(
            TopicRegistryView topicRegistry,
            ReactiveWorkerMessageRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        if (topicRegistry == null) {
            return Map.of();
        }
        Map<String, ReactiveWorkerMessageRepository> repositories = new LinkedHashMap<>();
        for (var topic : topicRegistry.topics()) {
            if (topic.table().isBlank()) {
                repositories.put(topic.kafkaTopic(), sharedRepository);
                continue;
            }
            if (repositoryFactory == null) {
                throw new IllegalStateException("TopicRepositoryFactory is required when topic '" + topic.kafkaTopic()
                        + "' uses dedicated table '" + topic.table() + "'");
            }
            repositories.put(topic.kafkaTopic(), repositoryFactory.createReactiveWorkerRepository(topic.table()));
        }
        return repositories;
    }

    private static Map<String, Long> normalizedCounts(Map<String, Long> counts) {
        Map<String, Long> normalized = new LinkedHashMap<>();
        normalized.put("READY", counts.getOrDefault("READY", 0L));
        normalized.put("FAILED", counts.getOrDefault("FAILED", 0L));
        normalized.put("STOPPED", counts.getOrDefault("STOPPED", 0L));
        normalized.put("PROCESSED", counts.getOrDefault("PROCESSED", 0L));
        return normalized;
    }
}
