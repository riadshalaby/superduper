package net.rsworld.superduper.worker.reactive;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;

public class ReactiveTopicWorkerCoordinator implements SmartLifecycle {
    private final List<ReactiveTopicWorkerInstance> workers;
    private final TaskScheduler taskScheduler;
    private final long claimIntervalMs;
    private final long claimInitialDelayMs;
    private final List<ScheduledFuture<?>> futures = new ArrayList<>();
    private volatile boolean running;

    public ReactiveTopicWorkerCoordinator(
            TopicRegistryView topicRegistry,
            ReactiveWorkerMessageRepository sharedRepository,
            LockingTaskExecutor lockExec,
            Map<String, ReactiveMessageHandler> handlers,
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
                        lockExec,
                        handlers,
                        observer,
                        repositoryFactory,
                        workerId,
                        lockAtMostForMs,
                        lockAtLeastForMs))
                .toList();
    }

    private ReactiveTopicWorkerInstance createWorker(
            TopicConfigView topic,
            ReactiveWorkerMessageRepository sharedRepository,
            LockingTaskExecutor lockExec,
            Map<String, ReactiveMessageHandler> handlers,
            SuperduperObserver observer,
            TopicRepositoryFactory repositoryFactory,
            String workerId,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        ReactiveMessageHandler handler = handlers.get(topic.handlerBeanName());
        if (handler == null) {
            throw new IllegalArgumentException(
                    "No ReactiveMessageHandler bean named '" + topic.handlerBeanName() + "'");
        }
        ReactiveWorkerMessageRepository repository = topic.table().isBlank()
                ? sharedRepository
                : repositoryFactory.createReactiveWorkerRepository(topic.table());
        return new ReactiveTopicWorkerInstance(
                topic, repository, lockExec, handler, observer, workerId, lockAtMostForMs, lockAtLeastForMs);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        for (ReactiveTopicWorkerInstance worker : workers) {
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
