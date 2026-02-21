package net.rsworld.superduper.worker.reactive;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class SuperDuperWorkerReactiveServiceTest {

    @Test
    void processOne_successAndRetryPaths() throws Exception {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.markProcessed(1L)).thenReturn(Mono.empty());
        when(mr.markStopped(2L, 2)).thenReturn(Mono.empty());

        ReactiveMessageHandler handler =
                row -> Mono.just(row.id() == 1L ? ProcessingResult.SUCCESS : ProcessingResult.RETRY);

        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                2);

        ClaimedMessage row1 = new ClaimedMessage(1L, "k1", "v", 0, "cid");
        ClaimedMessage row2 = new ClaimedMessage(2L, "k2", "v", 1, "cid");

        var m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", ClaimedMessage.class);
        m.setAccessible(true);
        ((Mono<Void>) m.invoke(svc, row1)).block();
        ((Mono<Void>) m.invoke(svc, row2)).block();

        verify(mr).markProcessed(1L);
        verify(mr).markStopped(2L, 2);
    }

    @Test
    void processOne_retryBelowLimit_requeuesReady() throws Exception {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.markReadyForRetry(10L, 1)).thenReturn(Mono.empty());

        ReactiveMessageHandler handler = row -> Mono.just(ProcessingResult.RETRY);
        SuperDuperWorkerReactiveService svc = new SuperDuperWorkerReactiveService(
                mr,
                lockExec,
                handler,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                100,
                3);

        ClaimedMessage row = new ClaimedMessage(10L, "k1", "v1", 0, "cid");
        var m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", ClaimedMessage.class);
        m.setAccessible(true);
        ((Mono<Void>) m.invoke(svc, row)).block();

        verify(mr).markReadyForRetry(10L, 1);
    }

    @Test
    void schedule_usesShedlockForClaimSection() {
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        when(mr.claimBatch(anyString(), anyInt(), anyInt())).thenReturn(Flux.empty());
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
}
