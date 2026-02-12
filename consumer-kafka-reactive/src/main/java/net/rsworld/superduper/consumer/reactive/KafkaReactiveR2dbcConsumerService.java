package net.rsworld.superduper.consumer.reactive;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

class KafkaReactiveR2dbcConsumerService {
    private static final String OCCURRED_AT_HEADER = "occurred_at";
    private static final String MODE_REACTIVE = "reactive";

    private final ReactiveMessageIngestRepository messageIngestRepository;
    private final SuperduperObserver observer;

    KafkaReactiveR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this.messageIngestRepository = messageIngestRepository;
        this.observer = observer;
    }

    @KafkaListener(
            topics = "#{'${superduper.kafka.topics:${superduper.kafka.topic}}'.split('\\\\s*,\\\\s*')}",
            containerFactory = "reactiveKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        long started = System.nanoTime();
        String key = consumerRecord.key() != null ? consumerRecord.key() : "default";
        int payloadSize =
                consumerRecord.value() == null ? 0 : consumerRecord.value().getBytes(StandardCharsets.UTF_8).length;
        observer.consumerReceived(new ConsumerObservation(
                MODE_REACTIVE,
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                key,
                payloadSize,
                0));
        String seed = consumerRecord.topic() + ":" + consumerRecord.partition() + ":" + consumerRecord.offset();
        Instant occurredAt = resolveOccurredAt(consumerRecord);
        messageIngestRepository
                .upsertReadyMessage(
                        UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8))
                                .toString(),
                        key,
                        consumerRecord.value(),
                        occurredAt)
                .doOnSuccess(unused -> {
                    ack.acknowledge();
                    observer.consumerSucceeded(new ConsumerObservation(
                            MODE_REACTIVE,
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            key,
                            payloadSize,
                            elapsedMs(started)));
                })
                .onErrorResume(e -> {
                    observer.consumerFailed(
                            new ConsumerObservation(
                                    MODE_REACTIVE,
                                    consumerRecord.topic(),
                                    consumerRecord.partition(),
                                    consumerRecord.offset(),
                                    key,
                                    payloadSize,
                                    elapsedMs(started)),
                            e);
                    return Mono.empty();
                })
                .subscribe();
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static Instant resolveOccurredAt(ConsumerRecord<String, String> consumerRecord) {
        Header header = consumerRecord.headers().lastHeader(OCCURRED_AT_HEADER);
        if (header != null) {
            Instant parsed = parseOccurredAtHeader(header.value());
            if (parsed != null) {
                return parsed;
            }
        }
        long timestamp = consumerRecord.timestamp();
        if (timestamp > 0) {
            return Instant.ofEpochMilli(timestamp);
        }
        return Instant.now();
    }

    private static Instant parseOccurredAtHeader(byte[] headerValue) {
        if (headerValue == null || headerValue.length == 0) {
            return null;
        }
        String raw = new String(headerValue, StandardCharsets.UTF_8).trim();
        if (raw.isEmpty()) {
            return null;
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(raw));
        } catch (NumberFormatException ignored) {
            // Continue with ISO-8601 parsing.
        }
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
