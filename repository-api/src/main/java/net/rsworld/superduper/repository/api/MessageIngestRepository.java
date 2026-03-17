package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Blocking repository contract for persisting newly consumed messages.
 *
 * <p>Consumers use this port to insert or upsert rows into the backing message store before workers attempt claims.
 */
public interface MessageIngestRepository {
    /**
     * Persists a ready message in the shared default topic.
     *
     * @param messageId the logical message id
     * @param messageKey the ordering key used by worker claims
     * @param content the serialized message payload
     * @param occurredAt the business occurrence timestamp
     * @param correlationId the correlation id associated with the message
     * @param messageType the application-defined message type
     */
    default void upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
    }

    /**
     * Persists a ready message for the given logical topic.
     *
     * @param topic the logical topic backing the target message table
     * @param messageId the logical message id
     * @param messageKey the ordering key used by worker claims
     * @param content the serialized message payload
     * @param occurredAt the business occurrence timestamp
     * @param correlationId the correlation id associated with the message
     * @param messageType the application-defined message type
     */
    void upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);

    /**
     * Persists a batch of ready messages in encounter order.
     *
     * @param messages the messages to insert or upsert
     * @throws NullPointerException if {@code messages} is {@code null}
     */
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
