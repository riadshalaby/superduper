package net.rsworld.superduper.repository.api;

import java.time.Instant;

public record MessageIngestData(
        String topic,
        String messageId,
        String messageKey,
        String content,
        Instant occurredAt,
        String correlationId,
        String messageType) {}
