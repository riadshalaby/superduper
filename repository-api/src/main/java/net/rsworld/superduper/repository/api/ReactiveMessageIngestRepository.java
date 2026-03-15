package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import reactor.core.publisher.Flux;
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
