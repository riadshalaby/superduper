package net.rsworld.superduper.repository.api;

public record ClaimedMessage(
        Long id,
        String messageId,
        String messageKey,
        String content,
        Integer retryCount,
        String containerId,
        String correlationId,
        String messageType) {}
