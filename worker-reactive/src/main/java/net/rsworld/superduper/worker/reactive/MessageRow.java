package net.rsworld.superduper.worker.reactive;

public record MessageRow(
        Long id, String uuid, String key, String content, String status, Integer retryCount, String containerId) {}
