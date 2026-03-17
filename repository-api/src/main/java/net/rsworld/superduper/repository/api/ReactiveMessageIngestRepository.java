package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository contract for persisting newly consumed messages.
 *
 * <p>Reactive consumer implementations use this port to store messages before they become eligible for worker claims.
 */
public interface ReactiveMessageIngestRepository {
    /**
     * Persists a ready message in the shared default topic.
     *
     * @param messageId the logical message id
     * @param messageKey the ordering key used by worker claims
     * @param content the serialized message payload
     * @param occurredAt the business occurrence timestamp
     * @param correlationId the correlation id associated with the message
     * @param messageType the application-defined message type
     * @return completion when the message has been stored
     */
    default Mono<Void> upsertReadyMessage(
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        return upsertReadyMessage("default", messageId, messageKey, content, occurredAt, correlationId, messageType);
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
     * @return completion when the message has been stored
     */
    Mono<Void> upsertReadyMessage(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType);

    /**
     * Persists a batch of ready messages sequentially.
     *
     * @param messages the messages to insert or upsert
     * @return completion when the entire batch has been stored
     * @throws NullPointerException if {@code messages} is {@code null}
     */
    default Mono<Void> batchUpsertReadyMessages(List<MessageIngestData> messages) {
        return Flux.fromIterable(Objects.requireNonNull(messages, "messages must not be null"))
                .concatMap(message -> upsertReadyMessage(
                        message.topic(),
                        message.messageId(),
                        message.messageKey(),
                        message.content(),
                        message.occurredAt(),
                        message.correlationId(),
                        message.messageType()))
                .then();
    }
}
