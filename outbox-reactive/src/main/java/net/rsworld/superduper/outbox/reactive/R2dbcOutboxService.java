package net.rsworld.superduper.outbox.reactive;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import reactor.core.publisher.Mono;

public class R2dbcOutboxService implements ReactiveOutboxService {
    private final Map<String, ReactiveMessageIngestRepository> repositories;

    @SuppressWarnings("unused")
    private final SuperduperObserver observer;

    public R2dbcOutboxService(Map<String, ReactiveMessageIngestRepository> repositories, SuperduperObserver observer) {
        this.repositories = Map.copyOf(Objects.requireNonNull(repositories, "repositories must not be null"));
        this.observer = observer == null ? NoopSuperduperObserver.INSTANCE : observer;
    }

    @Override
    public Mono<Void> send(String outboxName, String messageKey, String content) {
        return send(outboxName, messageKey, content, Instant.now(), null, null);
    }

    @Override
    public Mono<Void> send(
            String outboxName,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        ReactiveMessageIngestRepository repository = repositories.get(Objects.requireNonNull(outboxName, "outboxName"));
        if (repository == null) {
            return Mono.error(
                    new IllegalArgumentException("No outbox repository configured for outbox '" + outboxName + "'"));
        }

        return repository.upsertReadyMessage(
                outboxName,
                UUID.randomUUID().toString(),
                Objects.requireNonNull(messageKey, "messageKey"),
                Objects.requireNonNull(content, "content"),
                Objects.requireNonNull(occurredAt, "occurredAt"),
                correlationId,
                messageType);
    }
}
