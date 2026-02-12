package net.rsworld.superduper.observability.logging;

import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.ObservabilityComponent;
import net.rsworld.superduper.observability.api.ObservabilitySettings;
import net.rsworld.superduper.observability.api.ObservabilitySignal;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.observability.api.WorkerObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSuperduperObserver implements SuperduperObserver {
    private static final Logger log = LoggerFactory.getLogger("net.rsworld.superduper.observability");

    private final ObservabilitySettings settings;

    public LoggingSuperduperObserver(ObservabilitySettings settings) {
        this.settings = settings;
    }

    @Override
    public void consumerReceived(ConsumerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        log.info(
                "consumer.received mode={} topic={} partition={} offset={} key={} payloadSize={}",
                observation.mode(),
                observation.topic(),
                observation.partition(),
                observation.offset(),
                observation.key(),
                observation.payloadSize());
    }

    @Override
    public void consumerSucceeded(ConsumerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        if (settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.TIMING)) {
            log.info(
                    "consumer.persisted mode={} topic={} partition={} offset={} durationMs={}",
                    observation.mode(),
                    observation.topic(),
                    observation.partition(),
                    observation.offset(),
                    observation.durationMs());
        } else {
            log.info(
                    "consumer.persisted mode={} topic={} partition={} offset={}",
                    observation.mode(),
                    observation.topic(),
                    observation.partition(),
                    observation.offset());
        }
    }

    @Override
    public void consumerFailed(ConsumerObservation observation, Throwable error) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.FAILURE)) {
            return;
        }
        log.error(
                "consumer.failed mode={} topic={} partition={} offset={} durationMs={}",
                observation.mode(),
                observation.topic(),
                observation.partition(),
                observation.offset(),
                observation.durationMs(),
                error);
    }

    @Override
    public void workerClaimed(WorkerObservation observation, int claimedCount) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        if (settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.TIMING)) {
            log.info(
                    "worker.claimed mode={} workerId={} claimedCount={} batchSize={} durationMs={}",
                    observation.mode(),
                    observation.workerId(),
                    claimedCount,
                    observation.batchSize(),
                    observation.durationMs());
        } else {
            log.info(
                    "worker.claimed mode={} workerId={} claimedCount={} batchSize={}",
                    observation.mode(),
                    observation.workerId(),
                    claimedCount,
                    observation.batchSize());
        }
    }

    @Override
    public void workerProcessed(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        log.info(
                "worker.processed mode={} workerId={} messageId={} retryCount={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.messageId(),
                observation.retryCount(),
                observation.durationMs());
    }

    @Override
    public void workerRetried(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.RETRY)) {
            return;
        }
        log.warn(
                "worker.retry mode={} workerId={} messageId={} retryCount={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.messageId(),
                observation.retryCount(),
                observation.durationMs());
    }

    @Override
    public void workerStopped(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        log.error(
                "worker.stopped mode={} workerId={} messageId={} retryCount={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.messageId(),
                observation.retryCount(),
                observation.durationMs());
    }

    @Override
    public void workerFailed(WorkerObservation observation, Throwable error) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        log.error(
                "worker.failed mode={} workerId={} messageId={} retryCount={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.messageId(),
                observation.retryCount(),
                observation.durationMs(),
                error);
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation) {
        if (!settings.logEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        log.info(
                "maintenance.ok mode={} workerId={} operation={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.operation(),
                observation.durationMs());
    }

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {
        if (!settings.logEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE)) {
            return;
        }
        log.error(
                "maintenance.failed mode={} workerId={} operation={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.operation(),
                observation.durationMs(),
                error);
    }
}
