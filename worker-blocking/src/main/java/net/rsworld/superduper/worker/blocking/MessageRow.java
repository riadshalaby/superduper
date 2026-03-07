package net.rsworld.superduper.worker.blocking;

public record MessageRow(
        Long id,
        String messageId,
        String messageKey,
        String content,
        String status,
        Integer retryCount,
        String containerId,
        String correlationId,
        String messageType) {}
