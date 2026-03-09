package net.rsworld.superduper.worker.blocking;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import org.junit.jupiter.api.Test;

class CleanupServiceTest {

    @Test
    void cleanup_emitsObserverEntriesForEachRetentionBucket() {
        WorkerMaintenanceRepository repository = mock(WorkerMaintenanceRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.deleteProcessedOlderThan(14)).thenReturn(3);
        when(repository.deleteStoppedOlderThan(30)).thenReturn(2);
        when(repository.deleteStaleHeartbeats(1)).thenReturn(4);

        CleanupService service = new CleanupService(repository, observer, 14, 30, 1);

        service.cleanup();

        verify(repository).deleteProcessedOlderThan(14);
        verify(repository).deleteStoppedOlderThan(30);
        verify(repository).deleteStaleHeartbeats(1);
        verify(observer).maintenanceCleanup(any(), eq(3));
        verify(observer).maintenanceCleanup(any(), eq(2));
        verify(observer).maintenanceCleanup(any(), eq(4));
    }
}
