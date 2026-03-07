package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class SuperDuperWorkerReactiveServiceTest {

    @Test
    void processOne_successAndFailurePaths() throws Exception {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.markProcessed(anyLong(), anyString())).thenReturn(Mono.just(true));
        when(mr.markStopped(anyLong(), anyInt(), anyString())).thenReturn(Mono.just(true));

        ReactiveMessageHandler handler =
                row -> Mono.just(row.id() == 1L ? ProcessingResult.SUCCESS : ProcessingResult.FAILURE);

        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                2);

        ClaimedMessage row1 = new ClaimedMessage(1L, "mid-1", "k1", "v", 0, "cid", null, null);
        ClaimedMessage row2 = new ClaimedMessage(2L, "mid-2", "k2", "v", 1, "cid", null, null);

        var m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", ClaimedMessage.class);
        m.setAccessible(true);
        ((Mono<Void>) m.invoke(svc, row1)).block();
        ((Mono<Void>) m.invoke(svc, row2)).block();

        verify(mr).markProcessed(eq(1L), anyString());
        verify(mr).markStopped(eq(2L), eq(2), anyString());
    }

    @Test
    void processOne_failureBelowLimit_marksFailed() throws Exception {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.markFailed(anyLong(), anyInt(), anyString())).thenReturn(Mono.just(true));

        ReactiveMessageHandler handler = row -> Mono.just(ProcessingResult.FAILURE);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        ClaimedMessage row = new ClaimedMessage(10L, "mid-10", "k1", "v1", 0, "cid", null, null);
        var m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", ClaimedMessage.class);
        m.setAccessible(true);
        ((Mono<Void>) m.invoke(svc, row)).block();

        verify(mr).markFailed(eq(10L), eq(1), anyString());
    }

    @Test
    void processOne_passesMessageIdToHandler() throws Exception {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        when(mr.markProcessed(anyLong(), anyString())).thenReturn(Mono.just(true));

        ReactiveMessageHandler handler = row -> {
            assertThat(row.messageId()).isEqualTo("mid-11");
            return Mono.just(ProcessingResult.SUCCESS);
        };

        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                mock(LockingTaskExecutor.class),
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        ClaimedMessage row = new ClaimedMessage(11L, "mid-11", "k1", "v1", 0, "cid", null, null);
        var m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", ClaimedMessage.class);
        m.setAccessible(true);
        ((Mono<Void>) m.invoke(svc, row)).block();

        verify(mr).markProcessed(eq(11L), anyString());
    }

    @Test
    void schedule_usesShedlockForClaimSection() {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.claimBatch(anyString(), anyInt(), anyInt())).thenReturn(Mono.just(0L));
        doAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());

        ReactiveMessageHandler handler = row -> Mono.just(ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        svc.schedule();

        verify(lockExec).executeWithLock(any(Runnable.class), any());
        verify(mr).claimBatch(anyString(), anyInt(), anyInt());
    }

    @Test
    void schedule_processesRowsOnVirtualThread() {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        AtomicBoolean executedOnVirtualThread = new AtomicBoolean(false);

        when(mr.claimBatch(anyString(), anyInt(), anyInt())).thenReturn(Mono.just(1L));
        when(mr.fetchClaimedForWorker(anyString()))
                .thenReturn(Flux.just(new ClaimedMessage(1L, "mid-1", "k1", "v1", 0, "cid", null, null)));
        when(mr.markProcessed(anyLong(), anyString())).thenReturn(Mono.just(true));
        doAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());

        ReactiveMessageHandler handler = row -> Mono.fromSupplier(() -> {
            executedOnVirtualThread.set(Thread.currentThread().isVirtual());
            return ProcessingResult.SUCCESS;
        });
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        svc.schedule();

        verify(mr).markProcessed(eq(1L), anyString());
        org.assertj.core.api.Assertions.assertThat(executedOnVirtualThread.get())
                .isTrue();
    }

    @Test
    void schedule_releasesRemainingRowsForKeyAfterFailure() {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);

        when(mr.claimBatch(anyString(), anyInt(), anyInt())).thenReturn(Mono.just(3L));
        when(mr.fetchClaimedForWorker(anyString()))
                .thenReturn(Flux.just(
                        new ClaimedMessage(1L, "mid-1", "k1", "v1", 0, "cid", null, null),
                        new ClaimedMessage(2L, "mid-2", "k1", "v2", 0, "cid", null, null),
                        new ClaimedMessage(3L, "mid-3", "k2", "v3", 0, "cid", null, null)));
        when(mr.markFailed(anyLong(), anyInt(), anyString())).thenReturn(Mono.just(true));
        when(mr.markProcessed(anyLong(), anyString())).thenReturn(Mono.just(true));
        when(mr.releaseMessages(any(), anyString())).thenReturn(Mono.just(1));
        doAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());

        ReactiveMessageHandler handler =
                row -> Mono.just("k1".equals(row.messageKey()) ? ProcessingResult.FAILURE : ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        svc.schedule();

        verify(mr).releaseMessages(eq(java.util.List.of(2L)), anyString());
        verify(mr).markProcessed(eq(3L), anyString());
    }
}
