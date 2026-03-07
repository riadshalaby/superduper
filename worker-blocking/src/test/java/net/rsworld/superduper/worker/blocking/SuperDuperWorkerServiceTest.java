package net.rsworld.superduper.worker.blocking;

import static org.assertj.core.api.Assertions.assertThat;
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

        List<ClaimedMessage> claimedRows = List.of(
                new ClaimedMessage(1L, "mid-1", "k1", "v1", 0, "cid", null, null),
                new ClaimedMessage(2L, "mid-2", "k2", "v2", 1, "cid", null, null));
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
    void process_passesMessageIdToHandler() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);
        when(messageRepository.fetchClaimedForWorker(any()))
                .thenReturn(List.of(new ClaimedMessage(10L, "mid-10", "k1", "v1", 0, "cid", null, null)));
        when(messageRepository.markProcessed(anyLong(), anyString())).thenReturn(true);

        MessageHandler handler = row -> {
            assertThat(row.messageId()).isEqualTo("mid-10");
            return ProcessingResult.SUCCESS;
        };

        SuperDuperWorkerService svc = new SuperDuperWorkerService(
                messageRepository,
                mock(PlatformTransactionManager.class),
                mock(LockingTaskExecutor.class),
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        svc.process();

        verify(messageRepository).markProcessed(eq(10L), anyString());
    }

    @Test
    void process_failureBelowLimit_marksFailed() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);
        when(messageRepository.fetchClaimedForWorker(any()))
                .thenReturn(List.of(new ClaimedMessage(10L, "mid-10", "k1", "v1", 0, "cid", null, null)));
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

    @Test
    void process_releasesRemainingRowsForKeyAfterFailure() {
        WorkerMessageRepository messageRepository = mock(WorkerMessageRepository.class);
        when(messageRepository.fetchClaimedForWorker(any()))
                .thenReturn(List.of(
                        new ClaimedMessage(10L, "mid-10", "k1", "v1", 0, "cid", null, null),
                        new ClaimedMessage(11L, "mid-11", "k1", "v2", 0, "cid", null, null),
                        new ClaimedMessage(12L, "mid-12", "k2", "v3", 0, "cid", null, null)));
        when(messageRepository.markFailed(anyLong(), anyInt(), anyString())).thenReturn(true);
        when(messageRepository.markProcessed(anyLong(), anyString())).thenReturn(true);
        when(messageRepository.releaseMessages(any(), anyString())).thenReturn(1);

        MessageHandler handler =
                row -> "k1".equals(row.messageKey()) ? ProcessingResult.FAILURE : ProcessingResult.SUCCESS;
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

        verify(messageRepository).releaseMessages(eq(List.of(11L)), anyString());
        verify(messageRepository).markProcessed(eq(12L), anyString());
    }
}
