package net.rsworld.superduper.repository.api;

import java.time.Instant;
import reactor.core.publisher.Mono;

public interface ReactiveMessageIngestRepository {
    Mono<Void> upsertReadyMessage(String uuid, String key, String content, Instant occurredAt);
}
