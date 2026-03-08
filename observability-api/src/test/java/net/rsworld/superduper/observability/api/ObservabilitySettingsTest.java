package net.rsworld.superduper.observability.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilitySettingsTest {

    @Test
    void defaults_enableExpectedValues() {
        ObservabilitySettings settings = ObservabilitySettings.defaults();

        assertThat(settings.enabled()).isTrue();
        assertThat(settings.consumerEnabled()).isTrue();
        assertThat(settings.workerEnabled()).isTrue();
        assertThat(settings.maintenanceEnabled()).isTrue();
        assertThat(settings.lifecycleEnabled()).isTrue();
        assertThat(settings.successEnabled()).isTrue();
        assertThat(settings.failureEnabled()).isTrue();
        assertThat(settings.retryEnabled()).isTrue();
        assertThat(settings.timingEnabled()).isTrue();
        assertThat(settings.logEnabled()).isTrue();
        assertThat(settings.metricsEnabled()).isFalse();
        assertThat(settings.metricsTopicTagEnabled()).isTrue();
        assertThat(settings.metricsExceptionTagEnabled()).isTrue();
    }

    @Test
    void allows_returnsFalseWhenGlobalObservabilityIsDisabled() {
        ObservabilitySettings settings = new ObservabilitySettings(
                false, true, true, true, true, true, true, true, true, true, true, true, true);

        for (ObservabilityComponent component : ObservabilityComponent.values()) {
            for (ObservabilitySignal signal : ObservabilitySignal.values()) {
                assertThat(settings.allows(component, signal)).isFalse();
            }
        }
    }

    @Test
    void allows_respectsComponentAndSignalFlags() {
        ObservabilitySettings settings = new ObservabilitySettings(
                true, false, true, false, false, true, true, false, true, true, true, true, true);

        assertThat(settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.SUCCESS))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE))
                .isFalse();

        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.RETRY))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS))
                .isTrue();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE))
                .isTrue();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.REDRIVE))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.TIMING))
                .isTrue();
    }

    @Test
    void noopObserver_acceptsAllCalls() {
        NoopSuperduperObserver observer = NoopSuperduperObserver.INSTANCE;

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 42L, "key-a", 12, 8L);
        WorkerObservation worker = new WorkerObservation("blocking", "worker-a", 99L, 2, 10, 16L);
        MaintenanceObservation maintenance = new MaintenanceObservation("blocking", "worker-a", "heartbeat", 5L);

        assertThat(NoopSuperduperObserver.INSTANCE).isSameAs(observer);
        assertThatCode(() -> {
                    observer.consumerReceived(consumer);
                    observer.consumerSucceeded(consumer);
                    observer.consumerFailed(consumer, new RuntimeException("consumer"));
                    observer.workerClaimed(worker, 3);
                    observer.workerProcessed(worker);
                    observer.workerBatchCompleted(worker, 1, 1, 1);
                    observer.workerRetried(worker);
                    observer.workerStopped(worker);
                    observer.workerRedriven(worker, 2);
                    observer.workerFailed(worker, new RuntimeException("worker"));
                    observer.maintenanceSucceeded(maintenance);
                    observer.maintenanceSucceeded(maintenance, 4);
                    observer.queueBacklogObserved("blocking", Map.of("READY", 2L));
                    observer.maintenanceFailed(maintenance, new RuntimeException("maintenance"));
                })
                .doesNotThrowAnyException();
    }
}
