package net.rsworld.superduper.repository.api;

import java.time.Instant;

public interface MessageIngestRepository {
    void upsertReadyMessage(String uuid, String key, String content, Instant occurredAt);
}
