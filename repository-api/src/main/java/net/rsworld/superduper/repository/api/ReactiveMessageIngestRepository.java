package net.rsworld.superduper.repository.api;

import reactor.core.publisher.Mono;

public interface ReactiveMessageIngestRepository {
    Mono<Void> upsertReadyMessage(String uuid, String key, String content);
}
