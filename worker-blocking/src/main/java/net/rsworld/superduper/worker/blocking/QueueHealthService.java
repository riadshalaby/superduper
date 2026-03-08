package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.scheduling.annotation.Scheduled;

/** Publishes periodic queue backlog snapshots for metrics backends. */
public class QueueHealthService {
    private static final String MODE_BLOCKING = "blocking";

    private final WorkerMessageRepository repository;
    private final SuperduperObserver observer;

    /**
     * Creates a queue health service using the current runtime id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     */
    public QueueHealthService(WorkerMessageRepository repository, SuperduperObserver observer) {
        this(repository, observer, ManagementFactory.getRuntimeMXBean().getName());
    }

    /**
     * Creates a queue health service.
     *
     * @param repository the message repository
     * @param observer the observability sink
     * @param ignoredWorkerId retained for constructor symmetry with other services
     */
    public QueueHealthService(WorkerMessageRepository repository, SuperduperObserver observer, String ignoredWorkerId) {
        this.repository = repository;
        this.observer = observer;
    }

    /** Polls current queue counts and publishes them to observability backends. */
    @Scheduled(
            fixedDelayString =
                    "${superduper.worker.queue-health.interval-ms:${superduper.worker.queue-health-interval-ms:60000}}")
    public void poll() {
        observer.queueBacklogObserved(MODE_BLOCKING, normalizedCounts(repository.countByStatus()));
    }

    private static Map<String, Long> normalizedCounts(Map<String, Long> counts) {
        Map<String, Long> normalized = new LinkedHashMap<>();
        normalized.put("READY", counts.getOrDefault("READY", 0L));
        normalized.put("FAILED", counts.getOrDefault("FAILED", 0L));
        normalized.put("STOPPED", counts.getOrDefault("STOPPED", 0L));
        normalized.put("PROCESSING", counts.getOrDefault("PROCESSING", 0L));
        return normalized;
    }
}
