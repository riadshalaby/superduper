package net.rsworld.superduper.example.seeder;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
class SeederRunner implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(SeederRunner.class);
    private static final int MIN_KEY_SHARE_DIVISOR = 10;
    private static final int RETRY_ONCE_EVERY = 200;
    private static final int ALWAYS_FAIL_EVERY = 1000;
    private static final String FAILURE_KEY = "order-0";

    private final String bootstrapServers;
    private final String topic;
    private final int count;
    private final int keys;
    private final String instanceId;

    SeederRunner(
            @Value("${superduper.seeder.bootstrap-servers}") String bootstrapServers,
            @Value("${superduper.seeder.topic:superduper.example}") String topic,
            @Value("${superduper.seeder.count:1000}") int count,
            @Value("${superduper.seeder.keys:20}") int keys,
            @Value("${superduper.seeder.instance-id:seeder-1}") String instanceId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.count = count;
        this.keys = keys;
        this.instanceId = instanceId;
    }

    @Override
    public void run(String... args) throws ExecutionException, InterruptedException {
        String runToken = UUID.randomUUID().toString();
        int effectiveKeys = effectiveKeyCount(count, keys);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        long startedAt = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String key = keyFor(i, effectiveKeys);
                String content = contentFor(i, runToken);
                producer.send(new ProducerRecord<>(topic, key, content)).get();
            }
            producer.flush();
        }

        long elapsedMs = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        log.info(
                "[Seeder] instance={} runToken={} published={} topic={} keys={} failureKey={} failurePattern=retry-once/every-{},always-fail/every-{} elapsedMs={}",
                instanceId,
                runToken,
                count,
                topic,
                effectiveKeys,
                FAILURE_KEY,
                RETRY_ONCE_EVERY,
                ALWAYS_FAIL_EVERY,
                elapsedMs);
    }

    static int effectiveKeyCount(int count, int configuredKeys) {
        return Math.max(Math.max(1, configuredKeys), ceilDiv(count, MIN_KEY_SHARE_DIVISOR));
    }

    static String keyFor(int i, int effectiveKeys) {
        if (isFailureIndex(i) || effectiveKeys == 1) {
            return FAILURE_KEY;
        }
        return "order-" + (1 + ((i - 1) % (effectiveKeys - 1)));
    }

    private String contentFor(int i, String runToken) {
        if (isAlwaysFailIndex(i)) {
            return "always-fail instance=" + instanceId + " runToken=" + runToken + " #" + i;
        }
        if (isRetryOnceIndex(i)) {
            return "retry-once instance=" + instanceId + " runToken=" + runToken + " #" + i;
        }
        return "ok instance=" + instanceId + " runToken=" + runToken + " #" + i;
    }

    static boolean isFailureIndex(int i) {
        return isAlwaysFailIndex(i) || isRetryOnceIndex(i);
    }

    private static boolean isAlwaysFailIndex(int i) {
        return i % ALWAYS_FAIL_EVERY == 0;
    }

    private static boolean isRetryOnceIndex(int i) {
        return i % RETRY_ONCE_EVERY == 0 && !isAlwaysFailIndex(i);
    }

    private static int ceilDiv(int dividend, int divisor) {
        return Math.max(1, (dividend + divisor - 1) / divisor);
    }
}
