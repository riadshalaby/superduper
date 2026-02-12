package net.rsworld.superduper.repository.api;

public record ClaimedMessage(Long id, String key, String content, Integer retryCount, String containerId) {}
