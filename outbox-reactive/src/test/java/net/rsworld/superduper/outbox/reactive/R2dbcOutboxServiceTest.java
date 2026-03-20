package net.rsworld.superduper.outbox.reactive;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class R2dbcOutboxServiceTest {

    @Test
    void send_routesToNamedRepositoryWithGeneratedUuid() {
        ReactiveMessageIngestRepository repository = mock(ReactiveMessageIngestRepository.class);
        when(repository.upsertReadyMessage(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());
        R2dbcOutboxService service =
                new R2dbcOutboxService(Map.of("orders-outbox", repository), NoopSuperduperObserver.INSTANCE);
        Instant occurredAt = Instant.parse("2026-03-20T08:30:00Z");

        StepVerifier.create(
                        service.send("orders-outbox", "order-123", "payload", occurredAt, "corr-1", "order.created"))
                .verifyComplete();

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
        ReactiveMessageIngestRepository repository = mock(ReactiveMessageIngestRepository.class);
        when(repository.upsertReadyMessage(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());
        R2dbcOutboxService service = new R2dbcOutboxService(Map.of("orders-outbox", repository), null);
        Instant before = Instant.now();

        StepVerifier.create(service.send("orders-outbox", "order-123", "payload"))
                .verifyComplete();

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
        reactor.test.StepVerifier.create(Mono.just(occurredAtCaptor.getValue()))
                .assertNext(occurredAt -> {
                    org.assertj.core.api.Assertions.assertThat(occurredAt.isBefore(before))
                            .isFalse();
                    org.assertj.core.api.Assertions.assertThat(occurredAt.isAfter(after))
                            .isFalse();
                })
                .verifyComplete();
    }

    @Test
    void send_throwsForUnknownOutboxName() {
        R2dbcOutboxService service =
                new R2dbcOutboxService(Map.of("orders-outbox", mock(ReactiveMessageIngestRepository.class)), null);

        StepVerifier.create(service.send("billing-outbox", "billing-1", "payload"))
                .expectErrorSatisfies(error -> org.assertj.core.api.Assertions.assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("billing-outbox"))
                .verify();
    }

    @Test
    void send_routesEachOutboxToItsMatchingRepository() {
        ReactiveMessageIngestRepository ordersRepository = mock(ReactiveMessageIngestRepository.class);
        ReactiveMessageIngestRepository billingRepository = mock(ReactiveMessageIngestRepository.class);
        when(billingRepository.upsertReadyMessage(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());
        R2dbcOutboxService service = new R2dbcOutboxService(
                Map.of("orders-outbox", ordersRepository, "billing-outbox", billingRepository),
                NoopSuperduperObserver.INSTANCE);

        StepVerifier.create(service.send(
                        "billing-outbox", "billing-1", "payload", Instant.parse("2026-03-20T08:45:00Z"), null, null))
                .verifyComplete();

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
