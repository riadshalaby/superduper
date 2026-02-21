package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.WorkerMaintenanceRepository;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.worker.jdbc.HeartbeatService;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.OrphanReclaimer;
import net.rsworld.superduper.worker.jdbc.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveHeartbeatService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.ReactiveOrphanReclaimer;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;

class AutoSelectConfigurationTest {

    @Test
    void createsJdbcBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        DataSource ds = mock(DataSource.class);
        LockProvider lp = cfg.lockProvider(ds);
        LockingTaskExecutor exec = cfg.lockingTaskExecutor(lp);

        WorkerMessageRepository mr = mock(WorkerMessageRepository.class);
        WorkerMaintenanceRepository maintenanceRepository = mock(WorkerMaintenanceRepository.class);
        PlatformTransactionManager txm = mock(PlatformTransactionManager.class);
        MessageHandler handler = row -> net.rsworld.superduper.worker.jdbc.ProcessingResult.SUCCESS;
        var observer = NoopSuperduperObserver.INSTANCE;

        SuperDuperWorkerService svc = cfg.jdbcWorker(mr, txm, exec, handler, observer, 100, 5);
        HeartbeatService hb = cfg.jdbcHeartbeatService(maintenanceRepository, observer, 30_000);
        OrphanReclaimer rec = cfg.jdbcOrphanReclaimer(maintenanceRepository, observer, 120_000, 30_000);

        assertThat(lp).isNotNull();
        assertThat(exec).isNotNull();
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();

        hb.heartbeat();
        rec.reclaim();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
    }

    @Test
    void createsReactiveBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        ReactiveWorkerMessageRepository mr = mock(ReactiveWorkerMessageRepository.class);
        ReactiveWorkerMaintenanceRepository maintenanceRepository = mock(ReactiveWorkerMaintenanceRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        ReactiveMessageHandler h = row ->
                reactor.core.publisher.Mono.just(net.rsworld.superduper.worker.reactive.ProcessingResult.SUCCESS);
        var observer = NoopSuperduperObserver.INSTANCE;
        SuperDuperWorkerReactiveService svc = cfg.reactiveWorker(mr, lockExec, h, observer, 100, 5);
        ReactiveHeartbeatService hb = cfg.reactiveHeartbeatService(maintenanceRepository, observer, 30_000);
        ReactiveOrphanReclaimer rec = cfg.reactiveOrphanReclaimer(maintenanceRepository, observer, 120_000, 30_000);
        assertThat(svc).isNotNull();
        assertThat(hb).isNotNull();
        assertThat(rec).isNotNull();

        when(maintenanceRepository.heartbeat(anyString())).thenReturn(reactor.core.publisher.Mono.empty());
        when(maintenanceRepository.reclaimStaleProcessing(anyInt())).thenReturn(reactor.core.publisher.Mono.empty());
        when(maintenanceRepository.reclaimMissingHeartbeats(anyInt())).thenReturn(reactor.core.publisher.Mono.empty());
        hb.heartbeat();
        rec.reclaim();
        verify(maintenanceRepository).heartbeat(anyString());
        verify(maintenanceRepository).reclaimStaleProcessing(anyInt());
        verify(maintenanceRepository).reclaimMissingHeartbeats(anyInt());
    }
}
