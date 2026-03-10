package net.rsworld.superduper.repository.api;

import java.time.Instant;
import reactor.core.publisher.Mono;

public interface ReactiveMessageIngestRepository {
    default Mono<Void> upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        return upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
    }

    Mono<Void> upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);
}
