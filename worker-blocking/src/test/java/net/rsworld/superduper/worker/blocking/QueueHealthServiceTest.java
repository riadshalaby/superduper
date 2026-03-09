package net.rsworld.superduper.worker.blocking;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.junit.jupiter.api.Test;

class QueueHealthServiceTest {

    @Test
    void pollPublishesProcessedInsteadOfProcessing() {
        WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.countByStatus()).thenReturn(Map.of("READY", 5L, "FAILED", 2L, "STOPPED", 1L, "PROCESSED", 8L));

        QueueHealthService service = new QueueHealthService(repository, observer);

        service.poll();

        verify(observer)
                .queueBacklogObserved(
                        eq("blocking"), eq(Map.of("READY", 5L, "FAILED", 2L, "STOPPED", 1L, "PROCESSED", 8L)));
    }
}
