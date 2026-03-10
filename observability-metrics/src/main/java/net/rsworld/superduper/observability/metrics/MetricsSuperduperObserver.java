package net.rsworld.superduper.observability.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.ObservabilityComponent;
import net.rsworld.superduper.observability.api.ObservabilitySettings;
import net.rsworld.superduper.observability.api.ObservabilitySignal;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import net.rsworld.superduper.observability.logging.LoggingSuperduperObserver;

public class MetricsSuperduperObserver implements SuperduperObserver {
    private static final String WORKER_PROCESS_DURATION = "superduper.worker.process.duration";
    private static final List<String> BACKLOG_STATUSES = List.of("READY", "FAILED", "STOPPED", "PROCESSED");

    private final MeterRegistry meterRegistry;
    private final ObservabilitySettings settings;
    private final LoggingSuperduperObserver loggingDelegate;
    private final Map<QueueGaugeKey, AtomicLong> queueBacklogGauges = new ConcurrentHashMap<>();

    public MetricsSuperduperObserver(MeterRegistry meterRegistry, ObservabilitySettings settings) {
        this.meterRegistry = meterRegistry;
        this.settings = settings;
        this.loggingDelegate = new LoggingSuperduperObserver(settings);
    }

    @Override
    public void consumerReceived(ConsumerObservation observation) {
        loggingDelegate.consumerReceived(observation);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        counter("superduper.consumer.received.total", consumerTags(observation, null))
                .increment();
    }

    @Override
    public void consumerSucceeded(ConsumerObservation observation) {
        loggingDelegate.consumerSucceeded(observation);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        Tags tags = consumerTags(observation, null);
        counter("superduper.consumer.persisted.total", tags).increment();
        recordDuration(
                "superduper.consumer.persist.duration",
                tags,
                observation.durationMs(),
                ObservabilityComponent.CONSUMER);
    }

    @Override
    public void consumerFailed(ConsumerObservation observation, Throwable error) {
        loggingDelegate.consumerFailed(observation, error);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.FAILURE)) {
            return;
        }
        Tags tags = consumerTags(observation, error);
        counter("superduper.consumer.failed.total", tags).increment();
        recordDuration(
                "superduper.consumer.persist.duration",
                tags,
                observation.durationMs(),
                ObservabilityComponent.CONSUMER);
    }

    @Override
    public void workerClaimed(WorkerObservation observation, int claimedCount) {
        loggingDelegate.workerClaimed(observation, claimedCount);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        Tags tags = workerTags(observation, null).and("claimed_count", Integer.toString(claimedCount));
        counter("superduper.worker.claim.total", tags).increment();
        recordDuration(
                "superduper.worker.claim.duration", tags, observation.durationMs(), ObservabilityComponent.WORKER);
    }

    @Override
    public void workerProcessed(WorkerObservation observation) {
        loggingDelegate.workerProcessed(observation);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        Tags tags = workerTags(observation, null);
        counter("superduper.worker.processed.total", tags).increment();
        recordDuration(WORKER_PROCESS_DURATION, tags, observation.durationMs(), ObservabilityComponent.WORKER);
    }

    @Override
    public void workerBatchCompleted(WorkerObservation observation, int processed, int failed, int stopped) {
        loggingDelegate.workerBatchCompleted(observation, processed, failed, stopped);
    }

    @Override
    public void workerRetried(WorkerObservation observation) {
        loggingDelegate.workerRetried(observation);
        if (!settings.metricsEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.RETRY)) {
            return;
        }
        Tags tags = workerTags(observation, null);
        counter("superduper.worker.retried.total", tags).increment();
        recordDuration(WORKER_PROCESS_DURATION, tags, observation.durationMs(), ObservabilityComponent.WORKER);
    }

    @Override
    public void workerStopped(WorkerObservation observation) {
        loggingDelegate.workerStopped(observation);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        Tags tags = workerTags(observation, null);
        counter("superduper.worker.stopped.total", tags).increment();
        recordDuration(WORKER_PROCESS_DURATION, tags, observation.durationMs(), ObservabilityComponent.WORKER);
    }

    @Override
    public void workerRedriven(WorkerObservation observation, int redrivenCount) {
        loggingDelegate.workerRedriven(observation, redrivenCount);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.REDRIVE)) {
            return;
        }
        counter("superduper.worker.redriven.total", workerTags(observation, null))
                .increment(redrivenCount);
    }

    @Override
    public void workerFailed(WorkerObservation observation, Throwable error) {
        loggingDelegate.workerFailed(observation, error);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        Tags tags = workerTags(observation, error);
        counter("superduper.worker.failed.total", tags).increment();
        recordDuration(WORKER_PROCESS_DURATION, tags, observation.durationMs(), ObservabilityComponent.WORKER);
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation) {
        maintenanceSucceeded(observation, 0);
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation, int reclaimedCount) {
        loggingDelegate.maintenanceSucceeded(observation, reclaimedCount);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        Tags tags = maintenanceTags(observation, null);
        counter("superduper.maintenance.total", tags.and("result", "success")).increment();
        counter("superduper.maintenance.reclaimed.total", tags).increment(reclaimedCount);
        recordDuration(
                "superduper.maintenance.duration", tags, observation.durationMs(), ObservabilityComponent.MAINTENANCE);
    }

    @Override
    public void maintenanceCleanup(MaintenanceObservation observation, int deletedCount) {
        loggingDelegate.maintenanceCleanup(observation, deletedCount);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        Counter.builder("superduper.maintenance.cleanup.total")
                .tags("mode", observation.mode(), "topic", observation.topic(), "operation", observation.operation())
                .register(meterRegistry)
                .increment(deletedCount);
    }

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {
        loggingDelegate.maintenanceFailed(observation, error);
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE)) {
            return;
        }
        Tags tags = maintenanceTags(observation, error);
        counter("superduper.maintenance.total", tags.and("result", "failure")).increment();
        recordDuration(
                "superduper.maintenance.duration", tags, observation.durationMs(), ObservabilityComponent.MAINTENANCE);
    }

    @Override
    public void queueBacklogObserved(String mode, String topic, Map<String, Long> statusCounts) {
        if (!settings.metricsEnabled()
                || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        for (String status : BACKLOG_STATUSES) {
            QueueGaugeKey key = new QueueGaugeKey(mode, topic, status);
            AtomicLong gaugeValue = queueBacklogGauges.computeIfAbsent(key, this::registerBacklogGauge);
            gaugeValue.set(statusCounts.getOrDefault(status, 0L));
        }
    }

    private void recordDuration(String name, Tags tags, long durationMs, ObservabilityComponent component) {
        if (!settings.allows(component, ObservabilitySignal.TIMING)) {
            return;
        }
        Timer.builder(name).tags(tags).register(meterRegistry).record(java.time.Duration.ofMillis(durationMs));
    }

    private Counter counter(String name, Tags tags) {
        return Counter.builder(name).tags(tags).register(meterRegistry);
    }

    private AtomicLong registerBacklogGauge(QueueGaugeKey key) {
        AtomicLong value = new AtomicLong();
        Gauge.builder("superduper.queue.backlog", value, AtomicLong::doubleValue)
                .tags("mode", key.mode(), "topic", key.topic(), "status", key.status())
                .register(meterRegistry);
        return value;
    }

    private Tags consumerTags(ConsumerObservation observation, Throwable error) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.of("mode", observation.mode()));
        if (settings.metricsTopicTagEnabled()) {
            tags.add(Tag.of("topic", observation.topic()));
        }
        addExceptionTag(tags, error);
        return Tags.of(tags);
    }

    private Tags workerTags(WorkerObservation observation, Throwable error) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.of("mode", observation.mode()));
        tags.add(Tag.of("topic", observation.topic()));
        addExceptionTag(tags, error);
        return Tags.of(tags);
    }

    private Tags maintenanceTags(MaintenanceObservation observation, Throwable error) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.of("mode", observation.mode()));
        tags.add(Tag.of("topic", observation.topic()));
        tags.add(Tag.of("operation", observation.operation()));
        addExceptionTag(tags, error);
        return Tags.of(tags);
    }

    private void addExceptionTag(List<Tag> tags, Throwable error) {
        if (!settings.metricsExceptionTagEnabled()) {
            return;
        }
        tags.add(Tag.of("exception", error == null ? "none" : error.getClass().getSimpleName()));
    }

    private record QueueGaugeKey(String mode, String topic, String status) {}
}
