package net.rsworld.superduper.repository.api;

public interface MessageIngestRepository {
    void upsertReadyMessage(String uuid, String key, String content);
}
