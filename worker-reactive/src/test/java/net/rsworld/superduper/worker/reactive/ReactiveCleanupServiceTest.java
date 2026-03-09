package net.rsworld.superduper.worker.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

class ReactiveCleanupServiceTest {

    @Test
    void cleanup_emitsObserverEntriesForEachRetentionBucket() {
        ReactiveWorkerMaintenanceRepository repository = mock(ReactiveWorkerMaintenanceRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.deleteProcessedOlderThan(14)).thenReturn(Mono.just(3));
        when(repository.deleteStoppedOlderThan(30)).thenReturn(Mono.just(2));
        when(repository.deleteStaleHeartbeats(1)).thenReturn(Mono.just(4));

        ReactiveCleanupService service = new ReactiveCleanupService(repository, observer, 14, 30, 1);

        service.cleanup();

        verify(repository).deleteProcessedOlderThan(14);
        verify(repository).deleteStoppedOlderThan(30);
        verify(repository).deleteStaleHeartbeats(1);
        verify(observer).maintenanceCleanup(any(), eq(3));
        verify(observer).maintenanceCleanup(any(), eq(2));
        verify(observer).maintenanceCleanup(any(), eq(4));
    }

    @Test
    void cleanup_reportsTheFailingRetentionBucket() {
        ReactiveWorkerMaintenanceRepository repository = mock(ReactiveWorkerMaintenanceRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        RuntimeException failure = new RuntimeException("boom");
        when(repository.deleteProcessedOlderThan(14)).thenReturn(Mono.just(3));
        when(repository.deleteStoppedOlderThan(30)).thenReturn(Mono.error(failure));

        ReactiveCleanupService service = new ReactiveCleanupService(repository, observer, 14, 30, 1);

        service.cleanup();

        ArgumentCaptor<MaintenanceObservation> observationCaptor =
                ArgumentCaptor.forClass(MaintenanceObservation.class);
        verify(observer).maintenanceFailed(observationCaptor.capture(), eq(failure));
        assertEquals("cleanup-stopped", observationCaptor.getValue().operation());
    }
}
