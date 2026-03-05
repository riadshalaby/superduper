package net.rsworld.superduper.example.blocking;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import net.rsworld.superduper.worker.blocking.SuperDuperWorkerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "superduper.example.seed.enabled", havingValue = "true")
class ExampleBlockingSeeder {
    private static final Logger log = LoggerFactory.getLogger(ExampleBlockingSeeder.class);

    private final String bootstrapServers;
    private final String topic;
    private final int count;
    private final int keyCount;
    private final long awaitTimeoutSeconds;
    private final long readinessTimeoutSeconds;
    private final SeedProgress progress;
    private final ObjectProvider<SuperDuperWorkerService> workerProvider;
    private final ObjectProvider<KafkaListenerEndpointRegistry> listenerRegistryProvider;

    ExampleBlockingSeeder(
            @Value("${superduper.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${superduper.kafka.topics:${superduper.kafka.topic}}") String configuredTopics,
            @Value("${superduper.example.seed.count:1000}") int count,
            @Value("${superduper.example.seed.keys:20}") int keyCount,
            @Value("${superduper.example.seed.await-timeout-seconds:180}") long awaitTimeoutSeconds,
            @Value("${superduper.example.seed.readiness-timeout-seconds:30}") long readinessTimeoutSeconds,
            SeedProgress progress,
            ObjectProvider<SuperDuperWorkerService> workerProvider,
            ObjectProvider<KafkaListenerEndpointRegistry> listenerRegistryProvider) {
        this.bootstrapServers = bootstrapServers;
        this.topic = firstConfiguredTopic(configuredTopics);
        this.count = count;
        this.keyCount = keyCount;
        this.awaitTimeoutSeconds = awaitTimeoutSeconds;
        this.readinessTimeoutSeconds = readinessTimeoutSeconds;
        this.progress = progress;
        this.workerProvider = workerProvider;
        this.listenerRegistryProvider = listenerRegistryProvider;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() throws InterruptedException, ExecutionException {
        ensureWorkerAvailable();
        waitForConsumerReadiness();
        String runToken = UUID.randomUUID().toString();
        progress.startRun(count, runToken);
        log.info("[Seeder] Initialized seed runToken={} expected={}.", runToken, count);
        publishSeedMessages(runToken);
        waitForHandlerCompletion();
    }

    private void ensureWorkerAvailable() {
        SuperDuperWorkerService worker = workerProvider.getIfAvailable();
        if (worker == null) {
            throw new IllegalStateException(
                    "Blocking worker bean is missing. Ensure superduper.consumer.type=spring and JDBC is configured.");
        }
        log.info(
                "[Seeder] Application ready. Worker bean active: {}",
                worker.getClass().getSimpleName());
    }

    private void waitForConsumerReadiness() throws InterruptedException {
        KafkaListenerEndpointRegistry listenerRegistry = listenerRegistryProvider.getIfAvailable();
        if (listenerRegistry == null) {
            log.warn(
                    "[Seeder] KafkaListenerEndpointRegistry not available; skipping listener assignment readiness wait.");
            return;
        }

        long timeoutNanos = TimeUnit.SECONDS.toNanos(readinessTimeoutSeconds);
        long deadline = System.nanoTime() + timeoutNanos;
        long runningSince = -1L;

        while (System.nanoTime() < deadline) {
            Collection<MessageListenerContainer> containers = listenerRegistry.getListenerContainers();
            boolean hasContainer = !containers.isEmpty();
            boolean allRunning = hasContainer && containers.stream().allMatch(MessageListenerContainer::isRunning);
            boolean allAssigned = hasContainer && containers.stream().allMatch(this::hasAssignedPartitions);
            if (allRunning && allAssigned) {
                log.info("[Seeder] Kafka listener container(s) are running with partition assignments; starting seed.");
                return;
            }
            runningSince = updateRunningSince(allRunning, runningSince);
            if (isReadinessGraceElapsed(runningSince)) {
                long runningMillis =
                        Duration.ofNanos(System.nanoTime() - runningSince).toMillis();
                log.warn(
                        "[Seeder] Kafka listener container(s) are running but partition assignment is still empty. "
                                + "Proceeding with seed after {}ms readiness grace period.",
                        runningMillis);
                return;
            }
            pauseMillis(200);
        }

        throw new IllegalStateException(
                "Timed out waiting for Kafka listener container readiness and partition assignment.");
    }

    private static long updateRunningSince(boolean allRunning, long runningSince) {
        if (!allRunning) {
            return -1L;
        }
        return runningSince < 0 ? System.nanoTime() : runningSince;
    }

    private static boolean isReadinessGraceElapsed(long runningSince) {
        if (runningSince < 0) {
            return false;
        }
        return Duration.ofNanos(System.nanoTime() - runningSince).toMillis() >= 2_000;
    }

    private boolean hasAssignedPartitions(MessageListenerContainer container) {
        try {
            Object assigned =
                    container.getClass().getMethod("getAssignedPartitions").invoke(container);
            return assigned instanceof Collection<?> c && !c.isEmpty();
        } catch (ReflectiveOperationException ex) {
            return container.isRunning();
        }
    }

    private void publishSeedMessages(String runToken) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        long startedAt = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String key = "order-" + (i % keyCount);
                String content = contentFor(i, runToken);
                producer.send(new ProducerRecord<>(topic, key, content)).get();
            }
            producer.flush();
        }

        long millis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        log.info(
                "[Seeder] Published {} test messages to topic={} in {}ms (keys={}, failure-pattern: retry-once/every-10, always-fail/every-40)",
                count,
                topic,
                millis,
                keyCount);
    }

    private void waitForHandlerCompletion() throws InterruptedException {
        log.info(
                "[Seeder] Waiting until MessageHandler has seen {} unique message ids (timeout={}s)...",
                progress.expectedCount(),
                awaitTimeoutSeconds);
        boolean completed = progress.await(awaitTimeoutSeconds);
        if (completed) {
            log.info("[Seeder] Completed: handler received {} unique messages.", progress.handledCount());
        } else {
            log.warn(
                    "[Seeder] Timeout: handler received {}/{} unique messages.",
                    progress.handledCount(),
                    progress.expectedCount());
        }
    }

    private static String contentFor(int i, String runToken) {
        if (i % 40 == 0) {
            return "always-fail runToken=" + runToken + " #" + i;
        }
        if (i % 10 == 0) {
            return "retry-once runToken=" + runToken + " #" + i;
        }
        return "ok runToken=" + runToken + " #" + i;
    }

    private static String firstConfiguredTopic(String configuredTopics) {
        if (configuredTopics == null) {
            return "";
        }
        int comma = configuredTopics.indexOf(',');
        String candidate = comma < 0 ? configuredTopics : configuredTopics.substring(0, comma);
        return candidate.trim();
    }

    private static void pauseMillis(long millis) {
        LockSupport.parkNanos(Duration.ofMillis(millis).toNanos());
    }
}
