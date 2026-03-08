package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ReactiveRedriveServiceTest {

    @Test
    void inspect_validatesStatusAndDelegates() {
        ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        List<ClaimedMessage> rows = List.of(new ClaimedMessage(1L, "mid-1", "k1", "v1", 2, null, null, null));
        when(repository.findByStatus("FAILED", 10)).thenReturn(Flux.fromIterable(rows));
        ReactiveRedriveService service = new ReactiveRedriveService(repository, observer, "worker-a");

        assertThat(service.inspect("FAILED", 10).collectList().block()).isEqualTo(rows);
        verify(repository).findByStatus("FAILED", 10);
        assertThatThrownBy(() -> service.inspect("READY", 10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void redriveOne_emitsObserverWithCount() {
        ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.redriveById(11L)).thenReturn(Mono.just(1));
        ReactiveRedriveService service = new ReactiveRedriveService(repository, observer, "worker-a");

        assertThat(service.redriveOne(11L).block()).isTrue();

        verify(repository).redriveById(11L);
        verify(observer).workerRedriven(any(), eq(1));
    }

    @Test
    void redriveBatch_validatesStatusAndReturnsCount() {
        ReactiveWorkerMessageRepository repository = mock(ReactiveWorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.redriveByStatus("STOPPED", 5)).thenReturn(Mono.just(3));
        ReactiveRedriveService service = new ReactiveRedriveService(repository, observer, "worker-a");

        assertThat(service.redriveBatch("STOPPED", 5).block()).isEqualTo(3);

        verify(repository).redriveByStatus("STOPPED", 5);
        verify(observer).workerRedriven(any(), eq(3));
        assertThatThrownBy(() -> service.redriveBatch("PROCESSED", 5)).isInstanceOf(IllegalArgumentException.class);
    }
}
