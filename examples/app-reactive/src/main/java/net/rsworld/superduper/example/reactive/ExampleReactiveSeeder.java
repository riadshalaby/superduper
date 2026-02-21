package net.rsworld.superduper.example.reactive;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.rsworld.superduper.worker.reactive.SuperDuperWorkerReactiveService;
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
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "superduper.example.seed.enabled", havingValue = "true")
class ExampleReactiveSeeder {
    private static final Logger log = LoggerFactory.getLogger(ExampleReactiveSeeder.class);

    private final String bootstrapServers;
    private final String topic;
    private final int count;
    private final int keyCount;
    private final long awaitTimeoutSeconds;
    private final long readinessTimeoutSeconds;
    private final SeedProgress progress;
    private final ObjectProvider<SuperDuperWorkerReactiveService> workerProvider;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final DatabaseClient db;

    ExampleReactiveSeeder(
            @Value("${superduper.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${superduper.kafka.topics:${superduper.kafka.topic}}") String configuredTopics,
            @Value("${superduper.example.seed.count:1000}") int count,
            @Value("${superduper.example.seed.keys:20}") int keyCount,
            @Value("${superduper.example.seed.await-timeout-seconds:180}") long awaitTimeoutSeconds,
            @Value("${superduper.example.seed.readiness-timeout-seconds:30}") long readinessTimeoutSeconds,
            SeedProgress progress,
            ObjectProvider<SuperDuperWorkerReactiveService> workerProvider,
            KafkaListenerEndpointRegistry listenerRegistry,
            DatabaseClient db) {
        this.bootstrapServers = bootstrapServers;
        this.topic = configuredTopics.split("\\s*,\\s*")[0];
        this.count = count;
        this.keyCount = keyCount;
        this.awaitTimeoutSeconds = awaitTimeoutSeconds;
        this.readinessTimeoutSeconds = readinessTimeoutSeconds;
        this.progress = progress;
        this.workerProvider = workerProvider;
        this.listenerRegistry = listenerRegistry;
        this.db = db;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() throws Exception {
        ensureWorkerAvailable();
        waitForConsumerReadiness();
        prepareForNewSeedRun();
        int run = progress.activeRun();
        publishSeedMessages(run);
        waitForHandlerCompletion();
    }

    private void ensureWorkerAvailable() {
        SuperDuperWorkerReactiveService worker = workerProvider.getIfAvailable();
        if (worker == null) {
            throw new IllegalStateException(
                    "Reactive worker bean is missing. Ensure superduper.consumer.type=reactor and R2DBC is configured.");
        }
        log.info(
                "[Seeder] Application ready. Worker bean active: {}",
                worker.getClass().getSimpleName());
    }

    private void waitForConsumerReadiness() throws InterruptedException {
        long timeoutNanos = TimeUnit.SECONDS.toNanos(readinessTimeoutSeconds);
        long deadline = System.nanoTime() + timeoutNanos;

        while (System.nanoTime() < deadline) {
            Collection<MessageListenerContainer> containers = listenerRegistry.getListenerContainers();
            boolean hasContainer = !containers.isEmpty();
            boolean allRunning = hasContainer && containers.stream().allMatch(MessageListenerContainer::isRunning);
            boolean allAssigned = hasContainer && containers.stream().allMatch(this::hasAssignedPartitions);
            if (allRunning && allAssigned) {
                log.info("[Seeder] Kafka listener container(s) are running with partition assignments; starting seed.");
                return;
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException(
                "Timed out waiting for Kafka listener container readiness and partition assignment.");
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

    private void prepareForNewSeedRun() {
        db.sql("TRUNCATE TABLE messages RESTART IDENTITY").fetch().rowsUpdated().block();
        progress.startRun(count);
        log.info(
                "[Seeder] Cleared messages table and initialized seed run={} expected={}.",
                progress.activeRun(),
                count);
    }

    private void publishSeedMessages(int run) throws Exception {
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
                String content = contentFor(i, run);
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
                "[Seeder] Waiting until ReactiveMessageHandler has seen {} unique message ids (timeout={}s)...",
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

    private static String contentFor(int i, int run) {
        if (i % 40 == 0) {
            return "always-fail run=" + run + " #" + i;
        }
        if (i % 10 == 0) {
            return "retry-once run=" + run + " #" + i;
        }
        return "ok run=" + run + " #" + i;
    }
}
