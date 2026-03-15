package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

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

    default void batchUpsertReadyMessages(List<MessageIngestData> messages) {
        for (MessageIngestData message : Objects.requireNonNull(messages, "messages must not be null")) {
            upsertReadyMessage(
                    message.topic(),
                    message.messageId(),
                    message.messageKey(),
                    message.content(),
                    message.occurredAt(),
                    message.correlationId(),
                    message.messageType());
        }
    }
}
