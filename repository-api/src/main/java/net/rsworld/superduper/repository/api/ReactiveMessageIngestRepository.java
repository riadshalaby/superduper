package net.rsworld.superduper.repository.api;

import java.time.Instant;
import reactor.core.publisher.Mono;

public interface ReactiveMessageIngestRepository {
    Mono<Void> upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);
}
