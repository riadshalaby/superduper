package net.rsworld.superduper.worker.blocking;

public record MessageRow(
        Long id, String uuid, String key, String content, String status, Integer retryCount, String containerId) {}
