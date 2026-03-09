package net.rsworld.superduper.observability.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.ObservabilitySettings;
import net.rsworld.superduper.observability.api.WorkerObservation;
import org.junit.jupiter.api.Test;

class MetricsSuperduperObserverTest {

    @Test
    void recordsCountersAndTimersWhenEnabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsSuperduperObserver observer = new MetricsSuperduperObserver(registry, enabledSettings(true, true, true));

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);
        WorkerObservation worker = new WorkerObservation("blocking", "worker-a", 91L, 2, 10, 7L);
        MaintenanceObservation maintenance = new MaintenanceObservation("blocking", "worker-a", "heartbeat", 4L);

        observer.consumerReceived(consumer);
        observer.consumerSucceeded(consumer);
        observer.consumerFailed(consumer, new IllegalStateException("consumer"));
        observer.workerClaimed(worker, 5);
        observer.workerProcessed(worker);
        observer.workerBatchCompleted(worker, 3, 1, 1);
        observer.workerRetried(worker);
        observer.workerStopped(worker);
        observer.workerRedriven(worker, 2);
        observer.workerFailed(worker, new IllegalArgumentException("worker"));
        observer.maintenanceSucceeded(maintenance, 4);
        observer.maintenanceCleanup(new MaintenanceObservation("blocking", "worker-a", "cleanup-processed", 4L), 3);
        observer.maintenanceFailed(maintenance, new RuntimeException("maintenance"));
        observer.queueBacklogObserved("blocking", Map.of("READY", 7L, "FAILED", 2L, "STOPPED", 1L, "PROCESSED", 3L));

        assertThat(counterCount(
                        registry,
                        "superduper.consumer.received.total",
                        "mode",
                        "reactive",
                        "topic",
                        "topic-a",
                        "exception",
                        "none"))
                .isEqualTo(1.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.consumer.persisted.total",
                        "mode",
                        "reactive",
                        "topic",
                        "topic-a",
                        "exception",
                        "none"))
                .isEqualTo(1.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.consumer.failed.total",
                        "mode",
                        "reactive",
                        "topic",
                        "topic-a",
                        "exception",
                        "IllegalStateException"))
                .isEqualTo(1.0d);

        assertThat(counterCount(
                        registry,
                        "superduper.worker.claim.total",
                        "mode",
                        "blocking",
                        "exception",
                        "none",
                        "claimed_count",
                        "5"))
                .isEqualTo(1.0d);
        assertThat(counterCount(registry, "superduper.worker.processed.total", "mode", "blocking", "exception", "none"))
                .isEqualTo(1.0d);
        assertThat(counterCount(registry, "superduper.worker.retried.total", "mode", "blocking", "exception", "none"))
                .isEqualTo(1.0d);
        assertThat(counterCount(registry, "superduper.worker.stopped.total", "mode", "blocking", "exception", "none"))
                .isEqualTo(1.0d);
        assertThat(counterCount(registry, "superduper.worker.redriven.total", "mode", "blocking"))
                .isEqualTo(2.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.worker.failed.total",
                        "mode",
                        "blocking",
                        "exception",
                        "IllegalArgumentException"))
                .isEqualTo(1.0d);

        assertThat(counterCount(
                        registry,
                        "superduper.maintenance.total",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "exception",
                        "none",
                        "result",
                        "success"))
                .isEqualTo(1.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.maintenance.reclaimed.total",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "exception",
                        "none"))
                .isEqualTo(4.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.maintenance.total",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "exception",
                        "RuntimeException",
                        "result",
                        "failure"))
                .isEqualTo(1.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.maintenance.cleanup.total",
                        "mode",
                        "blocking",
                        "operation",
                        "cleanup-processed"))
                .isEqualTo(3.0d);

        assertThat(timerCount(
                        registry,
                        "superduper.consumer.persist.duration",
                        "mode",
                        "reactive",
                        "topic",
                        "topic-a",
                        "exception",
                        "none"))
                .isEqualTo(1L);
        assertThat(timerCount(
                        registry,
                        "superduper.consumer.persist.duration",
                        "mode",
                        "reactive",
                        "topic",
                        "topic-a",
                        "exception",
                        "IllegalStateException"))
                .isEqualTo(1L);
        assertThat(timerCount(
                        registry,
                        "superduper.worker.claim.duration",
                        "mode",
                        "blocking",
                        "exception",
                        "none",
                        "claimed_count",
                        "5"))
                .isEqualTo(1L);
        assertThat(timerCount(registry, "superduper.worker.process.duration", "mode", "blocking", "exception", "none"))
                .isEqualTo(3L);
        assertThat(timerCount(
                        registry,
                        "superduper.worker.process.duration",
                        "mode",
                        "blocking",
                        "exception",
                        "IllegalArgumentException"))
                .isEqualTo(1L);
        assertThat(timerCount(
                        registry,
                        "superduper.maintenance.duration",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "exception",
                        "none"))
                .isEqualTo(1L);
        assertThat(timerCount(
                        registry,
                        "superduper.maintenance.duration",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "exception",
                        "RuntimeException"))
                .isEqualTo(1L);
        assertThat(gaugeValue(registry, "superduper.queue.backlog", "mode", "blocking", "status", "READY"))
                .isEqualTo(7.0d);
        assertThat(gaugeValue(registry, "superduper.queue.backlog", "mode", "blocking", "status", "FAILED"))
                .isEqualTo(2.0d);
        assertThat(gaugeValue(registry, "superduper.queue.backlog", "mode", "blocking", "status", "STOPPED"))
                .isEqualTo(1.0d);
        assertThat(gaugeValue(registry, "superduper.queue.backlog", "mode", "blocking", "status", "PROCESSED"))
                .isEqualTo(3.0d);
    }

    @Test
    void doesNotRecordMetricsWhenDisabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsSuperduperObserver observer = new MetricsSuperduperObserver(registry, disabledMetricsSettings());

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);
        observer.consumerReceived(consumer);
        observer.consumerSucceeded(consumer);
        observer.consumerFailed(consumer, new RuntimeException("consumer"));

        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void omitsTopicExceptionTagsAndTimingWhenDisabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsSuperduperObserver observer =
                new MetricsSuperduperObserver(registry, enabledSettings(false, false, false));

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);
        WorkerObservation worker = new WorkerObservation("blocking", "worker-a", 91L, 2, 10, 7L);
        MaintenanceObservation maintenance = new MaintenanceObservation("blocking", "worker-a", "heartbeat", 4L);

        observer.consumerFailed(consumer, new IllegalStateException("consumer"));
        observer.workerClaimed(worker, 2);
        observer.maintenanceFailed(maintenance, new RuntimeException("maintenance"));

        assertThat(counterCount(registry, "superduper.consumer.failed.total", "mode", "reactive"))
                .isEqualTo(1.0d);
        assertThat(counterCount(registry, "superduper.worker.claim.total", "mode", "blocking", "claimed_count", "2"))
                .isEqualTo(1.0d);
        assertThat(counterCount(
                        registry,
                        "superduper.maintenance.total",
                        "mode",
                        "blocking",
                        "operation",
                        "heartbeat",
                        "result",
                        "failure"))
                .isEqualTo(1.0d);

        assertThat(registry.find("superduper.consumer.persist.duration").timer())
                .isNull();
        assertThat(registry.find("superduper.worker.claim.duration").timer()).isNull();
        assertThat(registry.find("superduper.maintenance.duration").timer()).isNull();
    }

    private static ObservabilitySettings enabledSettings(
            boolean timingEnabled, boolean topicTagEnabled, boolean exceptionTagEnabled) {
        return new ObservabilitySettings(
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                timingEnabled,
                false,
                true,
                topicTagEnabled,
                exceptionTagEnabled);
    }

    private static ObservabilitySettings disabledMetricsSettings() {
        return new ObservabilitySettings(
                true, true, true, true, true, true, true, true, true, false, false, true, true);
    }

    private static double counterCount(SimpleMeterRegistry registry, String name, String... tags) {
        return registry.find(name).tags(tags).counter().count();
    }

    private static long timerCount(SimpleMeterRegistry registry, String name, String... tags) {
        return registry.find(name).tags(tags).timer().count();
    }

    private static double gaugeValue(SimpleMeterRegistry registry, String name, String... tags) {
        return registry.find(name).tags(tags).gauge().value();
    }
}
