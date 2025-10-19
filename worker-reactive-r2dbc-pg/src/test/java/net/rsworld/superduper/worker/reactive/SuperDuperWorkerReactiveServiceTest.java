package net.rsworld.superduper.worker.reactive;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

class SuperDuperWorkerReactiveServiceTest {

    @Test
    void processOne_successAndRetryPaths() throws Exception {
        DatabaseClient db = mock(DatabaseClient.class);
        DatabaseClient.GenericExecuteSpec spec = mock(DatabaseClient.GenericExecuteSpec.class, RETURNS_DEEP_STUBS);

        when(db.sql(anyString())).thenReturn(spec);
        when(spec.bind(anyString(), Mockito.any())).thenReturn(spec);
        when(spec.fetch().rowsUpdated()).thenReturn(Mono.just(1));

        ReactiveMessageHandler handler =
                row -> Mono.just(row.id() == 1 ? ProcessingResult.SUCCESS : ProcessingResult.RETRY);

        SuperDuperWorkerReactiveService svc =
                new SuperDuperWorkerReactiveService(db, handler, 100, 2, 5000, 30000, 120000);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("key", "k1");
        row1.put("content", "v");
        row1.put("retry_count", 0);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2);
        row2.put("key", "k2");
        row2.put("content", "v");
        row2.put("retry_count", 1);

        Method m = SuperDuperWorkerReactiveService.class.getDeclaredMethod("processOne", Map.class);
        m.setAccessible(true);
        ((reactor.core.publisher.Mono<Void>) m.invoke(svc, row1)).block();
        ((reactor.core.publisher.Mono<Void>) m.invoke(svc, row2)).block();

        verify(db, atLeast(2)).sql(anyString());
    }
}
