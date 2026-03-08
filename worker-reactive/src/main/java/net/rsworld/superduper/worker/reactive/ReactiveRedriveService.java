package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Administrative service for inspecting and redriving failed worker messages. */
public class ReactiveRedriveService {
    private static final String MODE_REACTIVE = "reactive";

    private final ReactiveWorkerMessageRepository repository;
    private final SuperduperObserver observer;
    private final String workerId;

    /**
     * Creates a redrive service using the current runtime id as worker id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     */
    public ReactiveRedriveService(ReactiveWorkerMessageRepository repository, SuperduperObserver observer) {
        this(repository, observer, ManagementFactory.getRuntimeMXBean().getName());
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
        this.repository = repository;
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
    public Flux<ClaimedMessage> inspect(String status, int limit) {
        validateStatus(status);
        return repository.findByStatus(status, limit);
    }

    /**
     * Redrive a single message by id.
     *
     * @param id the message id
     * @return {@code true} if the message was redriven
     */
    public Mono<Boolean> redriveOne(long id) {
        long started = System.nanoTime();
        return repository.redriveById(id).map(redriven -> {
            observer.workerRedriven(
                    new WorkerObservation(MODE_REACTIVE, workerId, id, null, 1, elapsedMs(started)), redriven);
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
    public Mono<Integer> redriveBatch(String status, int limit) {
        validateStatus(status);
        long started = System.nanoTime();
        return repository
                .redriveByStatus(status, limit)
                .doOnNext(redriven -> observer.workerRedriven(
                        new WorkerObservation(MODE_REACTIVE, workerId, null, null, limit, elapsedMs(started)),
                        redriven));
    }

    private static void validateStatus(String status) {
        if (!"FAILED".equals(status) && !"STOPPED".equals(status)) {
            throw new IllegalArgumentException("status must be FAILED or STOPPED");
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
