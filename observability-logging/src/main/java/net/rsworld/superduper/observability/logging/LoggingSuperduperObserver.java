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
        if (!settings.logEnabled()
                || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        if (!log.isInfoEnabled()) {
            return;
        }
        String mode = observation.mode();
        String topic = observation.topic();
        int partition = observation.partition();
        long offset = observation.offset();
        String key = observation.key();
        int payloadSize = observation.payloadSize();
        log.info(
                "consumer.received mode={} topic={} partition={} offset={} key={} payloadSize={}",
                mode,
                topic,
                partition,
                offset,
                key,
                payloadSize);
    }

    @Override
    public void consumerSucceeded(ConsumerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        if (settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.TIMING)) {
            if (!log.isDebugEnabled()) {
                return;
            }
            String mode = observation.mode();
            String topic = observation.topic();
            int partition = observation.partition();
            long offset = observation.offset();
            long durationMs = observation.durationMs();
            log.debug(
                    "consumer.persisted mode={} topic={} partition={} offset={} durationMs={}",
                    mode,
                    topic,
                    partition,
                    offset,
                    durationMs);
        } else {
            if (!log.isDebugEnabled()) {
                return;
            }
            String mode = observation.mode();
            String topic = observation.topic();
            int partition = observation.partition();
            long offset = observation.offset();
            log.debug("consumer.persisted mode={} topic={} partition={} offset={}", mode, topic, partition, offset);
        }
    }

    @Override
    public void consumerFailed(ConsumerObservation observation, Throwable error) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.FAILURE)) {
            return;
        }
        if (!log.isErrorEnabled()) {
            return;
        }
        String mode = observation.mode();
        String topic = observation.topic();
        int partition = observation.partition();
        long offset = observation.offset();
        long durationMs = observation.durationMs();
        log.error(
                "consumer.failed mode={} topic={} partition={} offset={} durationMs={}",
                mode,
                topic,
                partition,
                offset,
                durationMs,
                error);
    }

    @Override
    public void workerClaimed(WorkerObservation observation, int claimedCount) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        if (!log.isInfoEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        int batchSize = observation.batchSize();
        if (settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.TIMING)) {
            long durationMs = observation.durationMs();
            log.info(
                    "worker.claimed mode={} workerId={} claimedCount={} batchSize={} durationMs={}",
                    mode,
                    workerId,
                    claimedCount,
                    batchSize,
                    durationMs);
        } else {
            log.info(
                    "worker.claimed mode={} workerId={} claimedCount={} batchSize={}",
                    mode,
                    workerId,
                    claimedCount,
                    batchSize);
        }
    }

    @Override
    public void workerProcessed(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        if (!log.isDebugEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        Long messageId = observation.messageId();
        Integer retryCount = observation.retryCount();
        long durationMs = observation.durationMs();
        log.debug(
                "worker.processed mode={} workerId={} messageId={} retryCount={} durationMs={}",
                mode,
                workerId,
                messageId,
                retryCount,
                durationMs);
    }

    @Override
    public void workerBatchCompleted(WorkerObservation observation, int processed, int failed, int stopped) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS)) {
            return;
        }
        if (!log.isInfoEnabled()) {
            return;
        }
        log.info(
                "worker.batch.completed mode={} workerId={} batchSize={} processed={} failed={} stopped={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                observation.batchSize(),
                processed,
                failed,
                stopped,
                observation.durationMs());
    }

    @Override
    public void workerRetried(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.RETRY)) {
            return;
        }
        if (!log.isWarnEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        Long messageId = observation.messageId();
        Integer retryCount = observation.retryCount();
        long durationMs = observation.durationMs();
        log.warn(
                "worker.retry mode={} workerId={} messageId={} retryCount={} durationMs={}",
                mode,
                workerId,
                messageId,
                retryCount,
                durationMs);
    }

    @Override
    public void workerStopped(WorkerObservation observation) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        if (!log.isErrorEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        Long messageId = observation.messageId();
        Integer retryCount = observation.retryCount();
        long durationMs = observation.durationMs();
        log.error(
                "worker.stopped mode={} workerId={} messageId={} retryCount={} durationMs={}",
                mode,
                workerId,
                messageId,
                retryCount,
                durationMs);
    }

    @Override
    public void workerFailed(WorkerObservation observation, Throwable error) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE)) {
            return;
        }
        if (!log.isErrorEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        Long messageId = observation.messageId();
        Integer retryCount = observation.retryCount();
        long durationMs = observation.durationMs();
        log.error(
                "worker.failed mode={} workerId={} messageId={} retryCount={} durationMs={}",
                mode,
                workerId,
                messageId,
                retryCount,
                durationMs,
                error);
    }

    @Override
    public void workerRedriven(WorkerObservation observation, int redrivenCount) {
        if (!settings.logEnabled() || !settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.REDRIVE)) {
            return;
        }
        if (!log.isInfoEnabled()) {
            return;
        }
        log.info(
                "worker.redriven mode={} workerId={} redrivenCount={} durationMs={}",
                observation.mode(),
                observation.workerId(),
                redrivenCount,
                observation.durationMs());
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation) {
        maintenanceSucceeded(observation, 0);
    }

    @Override
    public void maintenanceSucceeded(MaintenanceObservation observation, int reclaimedCount) {
        if (!settings.logEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.LIFECYCLE)) {
            return;
        }
        if (!log.isInfoEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        String operation = observation.operation();
        long durationMs = observation.durationMs();
        log.info(
                "maintenance.ok mode={} workerId={} operation={} reclaimedCount={} durationMs={}",
                mode,
                workerId,
                operation,
                reclaimedCount,
                durationMs);
    }

    @Override
    public void maintenanceFailed(MaintenanceObservation observation, Throwable error) {
        if (!settings.logEnabled()
                || !settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE)) {
            return;
        }
        if (!log.isErrorEnabled()) {
            return;
        }
        String mode = observation.mode();
        String workerId = observation.workerId();
        String operation = observation.operation();
        long durationMs = observation.durationMs();
        log.error(
                "maintenance.failed mode={} workerId={} operation={} durationMs={}",
                mode,
                workerId,
                operation,
                durationMs,
                error);
    }
}
