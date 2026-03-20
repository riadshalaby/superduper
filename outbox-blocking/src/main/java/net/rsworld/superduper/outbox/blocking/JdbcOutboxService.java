package net.rsworld.superduper.outbox.blocking;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;

public class JdbcOutboxService implements OutboxService {
    private final Map<String, MessageIngestRepository> repositories;

    @SuppressWarnings("unused")
    private final SuperduperObserver observer;

    public JdbcOutboxService(Map<String, MessageIngestRepository> repositories, SuperduperObserver observer) {
        this.repositories = Map.copyOf(Objects.requireNonNull(repositories, "repositories must not be null"));
        this.observer = observer == null ? NoopSuperduperObserver.INSTANCE : observer;
    }

    @Override
    public void send(String outboxName, String messageKey, String content) {
        send(outboxName, messageKey, content, Instant.now(), null, null);
    }

    @Override
    public void send(
            String outboxName,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {
        MessageIngestRepository repository = repositories.get(Objects.requireNonNull(outboxName, "outboxName"));
        if (repository == null) {
            throw new IllegalArgumentException("No outbox repository configured for outbox '" + outboxName + "'");
        }

        repository.upsertReadyMessage(
                outboxName,
                UUID.randomUUID().toString(),
                Objects.requireNonNull(messageKey, "messageKey"),
                Objects.requireNonNull(content, "content"),
                Objects.requireNonNull(occurredAt, "occurredAt"),
                correlationId,
                messageType);
    }
}
