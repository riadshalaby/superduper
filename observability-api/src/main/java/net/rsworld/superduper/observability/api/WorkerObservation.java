package net.rsworld.superduper.observability.api;

public record WorkerObservation(
        String mode,
        String topic,
        String workerId,
        Long messageId,
        Integer retryCount,
        Integer batchSize,
        long durationMs) {}
