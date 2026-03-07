package net.rsworld.superduper.repository.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultConsumerMetadataResolverTest {

    private final DefaultConsumerMetadataResolver resolver = new DefaultConsumerMetadataResolver();

    @Test
    void resolveMessageId_prefersHeaderValue() {
        String messageId = resolver.resolveMessageId(
                "topic-a", 1, 5L, Map.of("message_id", "message-5".getBytes(StandardCharsets.UTF_8)));

        assertThat(messageId).isEqualTo("message-5");
    }

    @Test
    void resolveMessageId_fallsBackToDeterministicUuid() {
        String messageId = resolver.resolveMessageId("topic-a", 1, 5L, Map.of());

        assertThat(messageId)
                .isEqualTo(UUID.nameUUIDFromBytes("topic-a:1:5".getBytes(StandardCharsets.UTF_8))
                        .toString());
    }

    @Test
    void resolveOccurredAt_prefersHeaderThenTimestamp() {
        Instant fromHeader = resolver.resolveOccurredAt(
                123L, Map.of("occurred_at", "2026-02-21T10:15:30Z".getBytes(StandardCharsets.UTF_8)));
        Instant fromTimestamp = resolver.resolveOccurredAt(1740132930000L, Map.of());

        assertThat(fromHeader).isEqualTo(Instant.parse("2026-02-21T10:15:30Z"));
        assertThat(fromTimestamp).isEqualTo(Instant.ofEpochMilli(1740132930000L));
    }

    @Test
    void resolveCorrelationId_andMessageType_useHeadersWhenPresent() {
        String correlationId =
                resolver.resolveCorrelationId(Map.of("correlationId", "corr-11".getBytes(StandardCharsets.UTF_8)));
        String messageType =
                resolver.resolveMessageType(Map.of("message_type", "invoice.created".getBytes(StandardCharsets.UTF_8)));

        assertThat(correlationId).isEqualTo("corr-11");
        assertThat(messageType).isEqualTo("invoice.created");
    }

    @Test
    void resolveCorrelationId_generatesUuidWhenHeaderMissing() {
        String correlationId = resolver.resolveCorrelationId(Map.of());

        assertThat(correlationId).isNotBlank();
        assertThatCode(() -> UUID.fromString(correlationId)).doesNotThrowAnyException();
    }
}
