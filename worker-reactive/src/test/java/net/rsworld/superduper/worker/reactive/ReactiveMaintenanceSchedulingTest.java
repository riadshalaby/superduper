package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ReactiveMaintenanceSchedulingTest {

    @Test
    void orphanReclaim_repeatedInvocationsDoNotOverlap() {
        ReactiveWorkerMaintenanceRepository maintenanceRepository = mock(ReactiveWorkerMaintenanceRepository.class);
        AtomicBoolean staleInFlight = new AtomicBoolean(false);
        AtomicBoolean heartbeatInFlight = new AtomicBoolean(false);
        AtomicBoolean overlapped = new AtomicBoolean(false);

        when(maintenanceRepository.reclaimStaleProcessing(anyInt()))
                .thenAnswer(inv -> guardedMono(staleInFlight, overlapped));
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt()))
                .thenAnswer(inv -> guardedMono(heartbeatInFlight, overlapped));

        ReactiveOrphanReclaimer reclaimer = new ReactiveOrphanReclaimer(
                maintenanceRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                120_000,
                90_000);

        reclaimer.reclaim();
        reclaimer.reclaim();

        assertThat(overlapped).isFalse();
        verify(maintenanceRepository, times(2)).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository, times(2)).reclaimMissingHeartbeats(anyInt());
    }

    @Test
    void heartbeat_repeatedInvocationsDoNotOverlap() {
        ReactiveWorkerMaintenanceRepository maintenanceRepository = mock(ReactiveWorkerMaintenanceRepository.class);
        AtomicBoolean heartbeatInFlight = new AtomicBoolean(false);
        AtomicBoolean overlapped = new AtomicBoolean(false);

        when(maintenanceRepository.heartbeat(anyString()))
                .thenAnswer(inv -> guardedMono(heartbeatInFlight, overlapped));

        ReactiveHeartbeatService heartbeatService = new ReactiveHeartbeatService(
                maintenanceRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);

        heartbeatService.heartbeat();
        heartbeatService.heartbeat();

        assertThat(overlapped).isFalse();
        verify(maintenanceRepository, times(2)).heartbeat(anyString());
    }

    private static Mono<Void> guardedMono(AtomicBoolean inFlight, AtomicBoolean overlapDetected) {
        return Mono.defer(() -> {
                    if (!inFlight.compareAndSet(false, true)) {
                        overlapDetected.set(true);
                    }
                    return Mono.delay(Duration.ofMillis(40)).then();
                })
                .doFinally(signalType -> inFlight.set(false));
    }
}
