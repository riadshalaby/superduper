package net.rsworld.superduper.observability.api;

public record ConsumerObservation(
        String mode,
        String topic,
        int partition,
        long offset,
        String key,
        int payloadSize,
        long durationMs) {}
