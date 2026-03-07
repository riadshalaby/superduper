package net.rsworld.superduper.repository.api;

import java.time.Instant;

public interface MessageIngestRepository {
    void upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);
}
