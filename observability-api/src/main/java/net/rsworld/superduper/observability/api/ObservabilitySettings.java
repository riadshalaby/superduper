package net.rsworld.superduper.observability.api;

public record ObservabilitySettings(
        boolean enabled,
        boolean consumerEnabled,
        boolean workerEnabled,
        boolean maintenanceEnabled,
        boolean lifecycleEnabled,
        boolean successEnabled,
        boolean failureEnabled,
        boolean retryEnabled,
        boolean timingEnabled,
        boolean logEnabled,
        boolean metricsEnabled,
        boolean metricsTopicTagEnabled,
        boolean metricsExceptionTagEnabled) {

    public static ObservabilitySettings defaults() {
        return new ObservabilitySettings(true, true, true, true, true, true, true, true, true, true, false, true, true);
    }

    public boolean allows(ObservabilityComponent component, ObservabilitySignal signal) {
        if (!enabled) {
            return false;
        }
        boolean componentAllowed =
                switch (component) {
                    case CONSUMER -> consumerEnabled;
                    case WORKER -> workerEnabled;
                    case MAINTENANCE -> maintenanceEnabled;
                };
        if (!componentAllowed) {
            return false;
        }
        return switch (signal) {
            case LIFECYCLE -> lifecycleEnabled;
            case SUCCESS -> successEnabled;
            case FAILURE -> failureEnabled;
            case RETRY -> retryEnabled;
            case REDRIVE -> lifecycleEnabled;
            case TIMING -> timingEnabled;
        };
    }
}
