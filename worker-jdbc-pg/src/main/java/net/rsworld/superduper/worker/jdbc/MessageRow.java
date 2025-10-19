package net.rsworld.superduper.worker.jdbc;

public record MessageRow(
        Integer id, String uuid, String key, String content, String status, Integer retryCount, String containerId) {}
