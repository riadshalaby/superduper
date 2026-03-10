package net.rsworld.superduper.repository.api;

public record ClaimedMessage(
        Long id,
        String topic,
        String messageId,
        String messageKey,
        String content,
        Integer retryCount,
        String containerId,
        String correlationId,
        String messageType) {
    public ClaimedMessage(
            Long id,
            String messageId,
            String messageKey,
            String content,
            Integer retryCount,
            String containerId,
            String correlationId,
            String messageType) {
        this(id, "default", messageId, messageKey, content, retryCount, containerId, correlationId, messageType);
    }
}
