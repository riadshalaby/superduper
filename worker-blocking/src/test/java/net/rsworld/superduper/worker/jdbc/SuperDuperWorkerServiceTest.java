package net.rsworld.superduper.worker.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;

class SuperDuperWorkerServiceTest {

    @Test
    void process_successAndRetryPaths() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);

        List<ClaimedMessage> claimedRows =
                List.of(new ClaimedMessage(1L, "k1", "v1", 0, "cid"), new ClaimedMessage(2L, "k2", "v2", 1, "cid"));
        when(messageRepository.fetchClaimedByIds(any())).thenReturn(claimedRows);

        MessageHandler handler = row -> row.id() == 1L ? ProcessingResult.SUCCESS : ProcessingResult.RETRY;
        PlatformTransactionManager txm = mock(PlatformTransactionManager.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);

        SuperDuperWorkerService svc = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                2);
        svc.process(List.of(1L, 2L));

        verify(messageRepository).markProcessed(1L);
        verify(messageRepository).markStopped(2L, 2);
    }

    @Test
    void process_retryBelowLimit_requeuesReady() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);
        when(messageRepository.fetchClaimedByIds(any()))
                .thenReturn(List.of(new ClaimedMessage(10L, "k1", "v1", 0, "cid")));

        MessageHandler handler = row -> ProcessingResult.RETRY;
        PlatformTransactionManager txm = mock(PlatformTransactionManager.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);

        SuperDuperWorkerService svc = new SuperDuperWorkerService(
                messageRepository,
                txm,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);
        svc.process(List.of(10L));

        verify(messageRepository).markReadyForRetry(10L, 1);
    }
}
