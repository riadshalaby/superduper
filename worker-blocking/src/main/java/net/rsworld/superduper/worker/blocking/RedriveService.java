package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import java.util.List;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;

/** Administrative service for inspecting and redriving failed worker messages. */
public class RedriveService {
    private static final String MODE_BLOCKING = "blocking";

    private final WorkerMessageRepository repository;
    private final SuperduperObserver observer;
    private final String workerId;

    /**
     * Creates a redrive service using the current runtime id as worker id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     */
    public RedriveService(WorkerMessageRepository repository, SuperduperObserver observer) {
        this(repository, observer, ManagementFactory.getRuntimeMXBean().getName());
    }

    /**
     * Creates a redrive service with an explicit worker id.
     *
     * @param repository the message repository
     * @param observer the observability sink
     * @param workerId the worker id to report in observations
     */
    public RedriveService(WorkerMessageRepository repository, SuperduperObserver observer, String workerId) {
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
    public List<ClaimedMessage> inspect(String status, int limit) {
        validateStatus(status);
        return repository.findByStatus(status, limit);
    }

    /**
     * Redrive a single message by id.
     *
     * @param id the message id
     * @return {@code true} if the message was redriven
     */
    public boolean redriveOne(long id) {
        long started = System.nanoTime();
        int redriven = repository.redriveById(id);
        observer.workerRedriven(
                new WorkerObservation(MODE_BLOCKING, workerId, id, null, 1, elapsedMs(started)), redriven);
        return redriven > 0;
    }

    /**
     * Redrive a batch of messages in the given status.
     *
     * @param status the terminal failure status to redrive
     * @param limit the maximum number of rows to update
     * @return the number of redriven rows
     */
    public int redriveBatch(String status, int limit) {
        validateStatus(status);
        long started = System.nanoTime();
        int redriven = repository.redriveByStatus(status, limit);
        observer.workerRedriven(
                new WorkerObservation(MODE_BLOCKING, workerId, null, null, limit, elapsedMs(started)), redriven);
        return redriven;
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
