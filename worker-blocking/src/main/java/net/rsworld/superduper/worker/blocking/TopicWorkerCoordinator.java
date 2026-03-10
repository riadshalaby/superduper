package net.rsworld.superduper.worker.blocking;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;

public class TopicWorkerCoordinator implements SmartLifecycle {
    private final List<TopicWorkerInstance> workers;
    private final TaskScheduler taskScheduler;
    private final long claimIntervalMs;
    private final long claimInitialDelayMs;
    private final List<ScheduledFuture<?>> futures = new ArrayList<>();
    private volatile boolean running;

    public TopicWorkerCoordinator(
            TopicRegistryView topicRegistry,
            WorkerMessageRepository sharedRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            Map<String, MessageHandler> handlers,
            SuperduperObserver observer,
            TopicRepositoryFactory repositoryFactory,
            TaskScheduler taskScheduler,
            long claimIntervalMs,
            long claimInitialDelayMs,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        this.taskScheduler = taskScheduler;
        this.claimIntervalMs = claimIntervalMs;
        this.claimInitialDelayMs = claimInitialDelayMs;
        String workerId = ManagementFactory.getRuntimeMXBean().getName();
        this.workers = topicRegistry.topics().stream()
                .map(topic -> createWorker(
                        topic,
                        sharedRepository,
                        txm,
                        lockExec,
                        handlers,
                        observer,
                        repositoryFactory,
                        workerId,
                        lockAtMostForMs,
                        lockAtLeastForMs))
                .toList();
    }

    private TopicWorkerInstance createWorker(
            TopicConfigView topic,
            WorkerMessageRepository sharedRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            Map<String, MessageHandler> handlers,
            SuperduperObserver observer,
            TopicRepositoryFactory repositoryFactory,
            String workerId,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        MessageHandler handler = handlers.get(topic.handlerBeanName());
        if (handler == null) {
            throw new IllegalArgumentException("No MessageHandler bean named '" + topic.handlerBeanName() + "'");
        }
        WorkerMessageRepository repository =
                topic.table().isBlank() ? sharedRepository : repositoryFactory.createWorkerRepository(topic.table());
        return new TopicWorkerInstance(
                topic, repository, txm, lockExec, handler, observer, workerId, lockAtMostForMs, lockAtLeastForMs);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        for (TopicWorkerInstance worker : workers) {
            futures.add(taskScheduler.scheduleWithFixedDelay(
                    worker::claimAndProcess,
                    Instant.now().plusMillis(claimInitialDelayMs),
                    Duration.ofMillis(claimIntervalMs)));
        }
        running = true;
    }

    @Override
    public void stop() {
        for (ScheduledFuture<?> future : futures) {
            future.cancel(false);
        }
        futures.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
