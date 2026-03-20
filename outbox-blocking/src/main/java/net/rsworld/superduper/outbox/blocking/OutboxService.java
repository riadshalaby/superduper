package net.rsworld.superduper.outbox.blocking;

import java.time.Instant;

/** Blocking API for inserting outbox rows that SUPERDUPER workers can later claim and process. */
public interface OutboxService {
    /**
     * Sends a message with the current instant as {@code occurredAt}.
     *
     * @param outboxName the configured outbox name
     * @param messageKey the ordering key used by workers
     * @param content the serialized payload
     */
    void send(String outboxName, String messageKey, String content);

    /**
     * Sends a message with explicit metadata.
     *
     * @param outboxName the configured outbox name
     * @param messageKey the ordering key used by workers
     * @param content the serialized payload
     * @param occurredAt the business occurrence timestamp
     * @param correlationId the correlation id associated with the message
     * @param messageType the application-defined message type
     */
    void send(
            String outboxName,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);
}
