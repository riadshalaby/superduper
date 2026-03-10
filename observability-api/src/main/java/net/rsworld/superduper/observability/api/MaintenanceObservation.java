package net.rsworld.superduper.observability.api;

public record MaintenanceObservation(String mode, String topic, String workerId, String operation, long durationMs) {}
