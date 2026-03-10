package net.rsworld.superduper.repository.api;

import java.time.Instant;

public interface MessageIngestRepository {
    default void upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
    }

    void upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);
}
