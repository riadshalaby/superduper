package net.rsworld.superduper.worker.reactive;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ReactiveQueueHealthServiceTest {

    @Test
    void pollPublishesProcessedInsteadOfProcessing() {
        ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.countByStatus())
                .thenReturn(Mono.just(Map.of("READY", 5L, "FAILED", 2L, "STOPPED", 1L, "PROCESSED", 8L)));

        ReactiveQueueHealthService service = new ReactiveQueueHealthService(repository, observer);

        service.poll();

        verify(observer)
                .queueBacklogObserved(
                        eq("reactive"), eq(Map.of("READY", 5L, "FAILED", 2L, "STOPPED", 1L, "PROCESSED", 8L)));
    }
}
