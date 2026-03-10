package net.rsworld.superduper.worker.reactive;

import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;

public class ReactiveTopicWorkerInstance {
    private final TopicConfigView topicConfig;
    private final SuperDuperWorkerReactiveService delegate;

    public ReactiveTopicWorkerInstance(
            TopicConfigView topicConfig,
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            String workerId) {
        this(topicConfig, messageRepository, lockExec, handler, observer, workerId, 15000, 2000);
    }

    ReactiveTopicWorkerInstance(
            TopicConfigView topicConfig,
            ReactiveWorkerMessageRepository messageRepository,
            LockingTaskExecutor lockExec,
            ReactiveMessageHandler handler,
            SuperduperObserver observer,
            String workerId,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        this.topicConfig = topicConfig;
        this.delegate = new SuperDuperWorkerReactiveService(
                messageRepository,
                lockExec,
                handler,
                observer,
                topicConfig.batchSize(),
                topicConfig.maxRetries(),
                topicConfig.claimLockName(),
                lockAtMostForMs,
                lockAtLeastForMs,
                workerId,
                topicConfig.kafkaTopic());
    }

    public void claimAndProcess() {
        delegate.schedule();
    }

    public TopicConfigView topicConfig() {
        return topicConfig;
    }
}
