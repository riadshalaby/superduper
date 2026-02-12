package net.rsworld.superduper.observability.api;

public record MaintenanceObservation(String mode, String workerId, String operation, long durationMs) {}
