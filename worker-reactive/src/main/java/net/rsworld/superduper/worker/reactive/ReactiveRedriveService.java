package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Administrative service for inspecting and redriving failed worker messages. */
public class ReactiveRedriveService {
    private static final String MODE_REACTIVE = "reactive";

    private final ReactiveWorkerMessageRepository repository;
    private final Map<String, ReactiveWorkerMessageRepository> repositoriesByTopic;
    private final TopicRegistryView topicRegistry;
    private final SuperduperObserver observer;
    private final String workerId;

    /**
     * Creates a redrive service using the current runtime id as worker id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     */
    public ReactiveRedriveService(ReactiveWorkerMessageRepository repository, SuperduperObserver observer) {
        this(
                repository,
                null,
                null,
                observer,
                ManagementFactory.getRuntimeMXBean().getName());
    }

    /**
     * Creates a redrive service with an explicit worker id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     * @param workerId the worker id to report in observations
     */
    public ReactiveRedriveService(
            ReactiveWorkerMessageRepository repository, SuperduperObserver observer, String workerId) {
        this(repository, null, null, observer, workerId);
    }

    public ReactiveRedriveService(
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

    public ReactiveRedriveService(
            ReactiveWorkerMessageRepository repository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            String workerId) {
        this.repository = repository;
        this.topicRegistry = topicRegistry;
        this.repositoriesByTopic = buildRepositories(topicRegistry, repository, repositoryFactory);
        this.observer = observer;
        this.workerId = workerId;
    }

    /**
     * Inspect messages in FAILED or STOPPED status.
     *
     * @param status the terminal failure status to inspect
     * @param limit the maximum number of rows to return
     * @return matching rows ordered by id
     */
    public Flux<ClaimedMessage> inspect(String status, int limit, String topic) {
        validateStatus(status);
        if (topicRegistry == null) {
            return repository.findByStatus(status, limit);
        }
        return repositoryFor(topic).findByStatus(status, limit, topic);
    }

    public Flux<ClaimedMessage> inspect(String status, int limit) {
        return inspect(status, limit, "default");
    }

    /**
     * Redrive a single message by id.
     *
     * @param id the message id
     * @return {@code true} if the message was redriven
     */
    public Mono<Boolean> redriveOne(long id) {
        long started = System.nanoTime();
        Mono<Integer> redriveMono = topicRegistry == null
                ? repository.redriveById(id)
                : Flux.fromIterable(new LinkedHashSet<>(repositoriesByTopic.values()))
                        .concatMap(candidate -> candidate.redriveById(id))
                        .filter(redriven -> redriven > 0)
                        .next()
                        .defaultIfEmpty(0);
        return redriveMono.map(redriven -> {
            observer.workerRedriven(
                    new WorkerObservation(MODE_REACTIVE, "all", workerId, id, null, 1, elapsedMs(started)), redriven);
            return redriven > 0;
        });
    }

    /**
     * Redrive a batch of messages in the given status.
     *
     * @param status the terminal failure status to redrive
     * @param limit the maximum number of rows to update
     * @return the number of redriven rows
     */
    public Mono<Integer> redriveBatch(String status, int limit, String topic) {
        validateStatus(status);
        long started = System.nanoTime();
        Mono<Integer> redriveMono = topicRegistry == null
                ? repository.redriveByStatus(status, limit)
                : repositoryFor(topic).redriveByStatus(status, limit, topic);
        return redriveMono.doOnNext(redriven -> observer.workerRedriven(
                new WorkerObservation(MODE_REACTIVE, topic, workerId, null, null, limit, elapsedMs(started)),
                redriven));
    }

    public Mono<Integer> redriveBatch(String status, int limit) {
        return redriveBatch(status, limit, "default");
    }

    private static void validateStatus(String status) {
        if (!"FAILED".equals(status) && !"STOPPED".equals(status)) {
            throw new IllegalArgumentException("status must be FAILED or STOPPED");
        }
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

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
