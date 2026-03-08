package net.rsworld.superduper.worker.blocking;

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
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import org.junit.jupiter.api.Test;

class RedriveServiceTest {

    @Test
    void inspect_validatesStatusAndDelegates() {
        WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        List<ClaimedMessage> rows = List.of(new ClaimedMessage(1L, "mid-1", "k1", "v1", 2, null, null, null));
        when(repository.findByStatus("FAILED", 10)).thenReturn(rows);
        RedriveService service = new RedriveService(repository, observer, "worker-a");

        assertThat(service.inspect("FAILED", 10)).isEqualTo(rows);
        verify(repository).findByStatus("FAILED", 10);
        assertThatThrownBy(() -> service.inspect("READY", 10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void redriveOne_emitsObserverWithCount() {
        WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.redriveById(11L)).thenReturn(1);
        RedriveService service = new RedriveService(repository, observer, "worker-a");

        assertThat(service.redriveOne(11L)).isTrue();

        verify(repository).redriveById(11L);
        verify(observer).workerRedriven(any(), eq(1));
    }

    @Test
    void redriveBatch_validatesStatusAndReturnsCount() {
        WorkerMessageRepository repository = mock(WorkerMessageRepository.class);
        SuperduperObserver observer = mock(SuperduperObserver.class);
        when(repository.redriveByStatus("STOPPED", 5)).thenReturn(3);
        RedriveService service = new RedriveService(repository, observer, "worker-a");

        assertThat(service.redriveBatch("STOPPED", 5)).isEqualTo(3);

        verify(repository).redriveByStatus("STOPPED", 5);
        verify(observer).workerRedriven(any(), eq(3));
        assertThatThrownBy(() -> service.redriveBatch("PROCESSED", 5)).isInstanceOf(IllegalArgumentException.class);
    }
}
