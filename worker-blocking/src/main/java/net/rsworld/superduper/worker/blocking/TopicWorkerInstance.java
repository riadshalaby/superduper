package net.rsworld.superduper.worker.blocking;

import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.springframework.transaction.PlatformTransactionManager;

public class TopicWorkerInstance {
    private final TopicConfigView topicConfig;
    private final SuperDuperWorkerService delegate;

    public TopicWorkerInstance(
            TopicConfigView topicConfig,
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            String workerId) {
        this(topicConfig, messageRepository, txm, lockExec, handler, observer, workerId, 15000, 2000);
    }

    TopicWorkerInstance(
            TopicConfigView topicConfig,
            WorkerMessageRepository messageRepository,
            PlatformTransactionManager txm,
            LockingTaskExecutor lockExec,
            MessageHandler handler,
            SuperduperObserver observer,
            String workerId,
            long lockAtMostForMs,
            long lockAtLeastForMs) {
        this.topicConfig = topicConfig;
        this.delegate = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                observer,
                topicConfig.batchSize(),
                topicConfig.maxRetries(),
                topicConfig.claimLockName(),
                lockAtMostForMs,
                lockAtLeastForMs,
                workerId,
                topicConfig.topicColumnValue());
    }

    public void claimAndProcess() {
        delegate.schedule();
    }

    public TopicConfigView topicConfig() {
        return topicConfig;
    }
}
