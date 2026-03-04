package net.rsworld.superduper.worker.blocking;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        when(messageRepository.markProcessed(anyLong(), anyString())).thenReturn(true);
        when(messageRepository.markStopped(anyLong(), anyInt(), anyString())).thenReturn(true);

        List<ClaimedMessage> claimedRows =
                List.of(new ClaimedMessage(1L, "k1", "v1", 0, "cid"), new ClaimedMessage(2L, "k2", "v2", 1, "cid"));
        when(messageRepository.fetchClaimedForWorker(any())).thenReturn(claimedRows);

        MessageHandler handler = row -> row.id() == 1L ? ProcessingResult.SUCCESS : ProcessingResult.FAILURE;
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
        svc.process();

        verify(messageRepository).markProcessed(eq(1L), anyString());
        verify(messageRepository).markStopped(eq(2L), eq(2), anyString());
    }

    @Test
    void process_failureBelowLimit_marksFailed() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);
        when(messageRepository.fetchClaimedForWorker(any()))
                .thenReturn(List.of(new ClaimedMessage(10L, "k1", "v1", 0, "cid")));
        when(messageRepository.markFailed(anyLong(), anyInt(), anyString())).thenReturn(true);

        MessageHandler handler = row -> ProcessingResult.FAILURE;
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
        svc.process();

        verify(messageRepository).markFailed(eq(10L), eq(1), anyString());
    }
}
