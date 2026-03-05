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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        long startedAt = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String key = "order-" + (i % keys);
                String content = contentFor(i, runToken);
                producer.send(new ProducerRecord<>(topic, key, content)).get();
            }
            producer.flush();
        }

        long elapsedMs = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        log.info(
                "[Seeder] instance={} runToken={} published={} topic={} elapsedMs={}",
                instanceId,
                runToken,
                count,
                topic,
                elapsedMs);
    }

    private String contentFor(int i, String runToken) {
        if (i % 40 == 0) {
            return "always-fail instance=" + instanceId + " runToken=" + runToken + " #" + i;
        }
        if (i % 10 == 0) {
            return "retry-once instance=" + instanceId + " runToken=" + runToken + " #" + i;
        }
        return "ok instance=" + instanceId + " runToken=" + runToken + " #" + i;
    }
}
