package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.SuperDuperWorkerService;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.PlatformTransactionManager;

class AutoSelectConfigurationTest {

    @Test
    void createsJdbcBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        DataSource ds = mock(DataSource.class);
        LockProvider lp = cfg.lockProvider(ds);
        LockingTaskExecutor exec = cfg.lockingTaskExecutor(lp);

        NamedParameterJdbcTemplate np = mock(NamedParameterJdbcTemplate.class);
        PlatformTransactionManager txm = mock(PlatformTransactionManager.class);
        MessageHandler handler = row -> net.rsworld.superduper.worker.jdbc.ProcessingResult.SUCCESS;

        SuperDuperWorkerService svc = cfg.jdbcWorker(np, txm, exec, handler);

        assertThat(lp).isNotNull();
        assertThat(exec).isNotNull();
        assertThat(svc).isNotNull();
    }

    @Test
    void createsReactiveBeans() {
        AutoSelectConfiguration cfg = new AutoSelectConfiguration();
        DatabaseClient db = mock(DatabaseClient.class);
        ReactiveMessageHandler h = row ->
                reactor.core.publisher.Mono.just(net.rsworld.superduper.worker.reactive.ProcessingResult.SUCCESS);
        SuperDuperWorkerReactiveService svc = cfg.reactiveWorker(db, h);
        assertThat(svc).isNotNull();
    }
}
