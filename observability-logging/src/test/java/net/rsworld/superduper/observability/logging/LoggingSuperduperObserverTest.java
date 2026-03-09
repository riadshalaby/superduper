package net.rsworld.superduper.observability.logging;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.MaintenanceObservation;
import net.rsworld.superduper.observability.api.ObservabilitySettings;
import net.rsworld.superduper.observability.api.WorkerObservation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class LoggingSuperduperObserverTest {

    private Logger logger;
    private Level originalLevel;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger("net.rsworld.superduper.observability");
        originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(originalLevel);
    }

    @Test
    void logsAllEventTypesWhenEnabled() {
        LoggingSuperduperObserver observer = new LoggingSuperduperObserver(enabledSettings(true));

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);
        WorkerObservation worker = new WorkerObservation("blocking", "worker-a", 91L, 2, 10, 7L);
        MaintenanceObservation maintenance = new MaintenanceObservation("blocking", "worker-a", "heartbeat", 4L);

        observer.consumerReceived(consumer);
        observer.consumerSucceeded(consumer);
        observer.consumerFailed(consumer, new RuntimeException("consumer"));
        observer.workerClaimed(worker, 5);
        observer.workerProcessed(worker);
        observer.workerBatchCompleted(worker, 3, 1, 1);
        observer.workerRetried(worker);
        observer.workerStopped(worker);
        observer.workerRedriven(worker, 2);
        observer.workerFailed(worker, new RuntimeException("worker"));
        observer.maintenanceSucceeded(maintenance, 4);
        observer.maintenanceCleanup(new MaintenanceObservation("blocking", "worker-a", "cleanup-processed", 4L), 3);
        observer.maintenanceFailed(maintenance, new RuntimeException("maintenance"));

        List<String> messages =
                appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(messages).hasSize(13);
        assertThat(messages).anyMatch(m -> m.startsWith("consumer.received"));
        assertThat(messages).anyMatch(m -> m.startsWith("consumer.persisted") && m.contains("durationMs="));
        assertThat(messages).anyMatch(m -> m.startsWith("consumer.failed"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.claimed") && m.contains("durationMs="));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.processed"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.batch.completed"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.retry"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.stopped"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.redriven"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.failed"));
        assertThat(messages).anyMatch(m -> m.startsWith("maintenance.ok") && m.contains("reclaimedCount=4"));
        assertThat(messages).anyMatch(m -> m.startsWith("maintenance.cleanup") && m.contains("deletedCount=3"));
        assertThat(messages).anyMatch(m -> m.startsWith("maintenance.failed"));
    }

    @Test
    void usesNonTimingTemplateWhenTimingSignalIsDisabled() {
        LoggingSuperduperObserver observer = new LoggingSuperduperObserver(enabledSettings(false));

        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);
        WorkerObservation worker = new WorkerObservation("blocking", "worker-a", 91L, 2, 10, 7L);

        observer.consumerSucceeded(consumer);
        observer.workerClaimed(worker, 5);

        List<String> messages =
                appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(messages).hasSize(2);
        assertThat(messages).allMatch(m -> !m.contains("durationMs="));
        assertThat(messages).anyMatch(m -> m.startsWith("consumer.persisted"));
        assertThat(messages).anyMatch(m -> m.startsWith("worker.claimed"));
    }

    @Test
    void skipsLogsWhenOutputOrSignalIsDisabled() {
        LoggingSuperduperObserver outputDisabled = new LoggingSuperduperObserver(new ObservabilitySettings(
                true, true, true, true, true, true, true, true, true, false, false, true, true));
        ConsumerObservation consumer = new ConsumerObservation("reactive", "topic-a", 1, 11L, "key-a", 8, 6L);

        outputDisabled.consumerReceived(consumer);
        outputDisabled.consumerFailed(consumer, new RuntimeException("consumer"));

        LoggingSuperduperObserver failureDisabled = new LoggingSuperduperObserver(new ObservabilitySettings(
                true, true, true, true, true, true, false, true, true, true, false, true, true));
        failureDisabled.consumerFailed(consumer, new RuntimeException("consumer"));

        assertThat(appender.list).isEmpty();
    }

    private static ObservabilitySettings enabledSettings(boolean timingEnabled) {
        return new ObservabilitySettings(
                true, true, true, true, true, true, true, true, timingEnabled, true, false, true, true);
    }
}
