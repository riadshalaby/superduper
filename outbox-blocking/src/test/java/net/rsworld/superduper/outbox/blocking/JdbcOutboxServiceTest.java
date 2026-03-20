package net.rsworld.superduper.outbox.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JdbcOutboxServiceTest {

    @Test
    void send_routesToNamedRepositoryWithGeneratedUuid() {
        MessageIngestRepository repository = mock(MessageIngestRepository.class);
        JdbcOutboxService service =
                new JdbcOutboxService(Map.of("orders-outbox", repository), NoopSuperduperObserver.INSTANCE);
        Instant occurredAt = Instant.parse("2026-03-20T08:30:00Z");

        service.send("orders-outbox", "order-123", "payload", occurredAt, "corr-1", "order.created");

        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(repository)
                .upsertReadyMessage(
                        eq("orders-outbox"),
                        messageIdCaptor.capture(),
                        eq("order-123"),
                        eq("payload"),
                        eq(occurredAt),
                        eq("corr-1"),
                        eq("order.created"));
        assertThatCode(() -> UUID.fromString(messageIdCaptor.getValue())).doesNotThrowAnyException();
    }

    @Test
    void send_simpleOverloadUsesCurrentInstantAndNullOptionalMetadata() {
        MessageIngestRepository repository = mock(MessageIngestRepository.class);
        JdbcOutboxService service = new JdbcOutboxService(Map.of("orders-outbox", repository), null);
        Instant before = Instant.now();

        service.send("orders-outbox", "order-123", "payload");

        Instant after = Instant.now();
        ArgumentCaptor<Instant> occurredAtCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(repository)
                .upsertReadyMessage(
                        eq("orders-outbox"),
                        any(String.class),
                        eq("order-123"),
                        eq("payload"),
                        occurredAtCaptor.capture(),
                        eq(null),
                        eq(null));
        assertThat(occurredAtCaptor.getValue().isBefore(before)).isFalse();
        assertThat(occurredAtCaptor.getValue().isAfter(after)).isFalse();
    }

    @Test
    void send_throwsForUnknownOutboxName() {
        JdbcOutboxService service =
                new JdbcOutboxService(Map.of("orders-outbox", mock(MessageIngestRepository.class)), null);

        assertThatThrownBy(() -> service.send("billing-outbox", "billing-1", "payload"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("billing-outbox");
    }

    @Test
    void send_routesEachOutboxToItsMatchingRepository() {
        MessageIngestRepository ordersRepository = mock(MessageIngestRepository.class);
        MessageIngestRepository billingRepository = mock(MessageIngestRepository.class);
        JdbcOutboxService service = new JdbcOutboxService(
                Map.of("orders-outbox", ordersRepository, "billing-outbox", billingRepository),
                NoopSuperduperObserver.INSTANCE);

        service.send("billing-outbox", "billing-1", "payload", Instant.parse("2026-03-20T08:45:00Z"), null, null);

        verify(billingRepository)
                .upsertReadyMessage(
                        eq("billing-outbox"),
                        any(String.class),
                        eq("billing-1"),
                        eq("payload"),
                        eq(Instant.parse("2026-03-20T08:45:00Z")),
                        eq(null),
                        eq(null));
        verify(ordersRepository, never())
                .upsertReadyMessage(
                        any(String.class),
                        any(String.class),
                        any(String.class),
                        any(String.class),
                        any(),
                        any(),
                        any());
    }
}
