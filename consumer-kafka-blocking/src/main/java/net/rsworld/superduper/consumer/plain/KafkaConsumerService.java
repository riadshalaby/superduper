package net.rsworld.superduper.consumer.plain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
class KafkaConsumerService {
    private static final String OCCURRED_AT_HEADER = "occurred_at";

    private final MessageIngestRepository messageIngestRepository;
    private final SuperduperObserver observer;

    KafkaConsumerService(MessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this.messageIngestRepository = messageIngestRepository;
        this.observer = observer;
    }

    @KafkaListener(topics = "${superduper.kafka.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        long started = System.nanoTime();
        String k = record.key() != null ? record.key() : "default";
        int payloadSize = record.value() == null ? 0 : record.value().getBytes(StandardCharsets.UTF_8).length;
        observer.consumerReceived(new ConsumerObservation(
                "blocking", record.topic(), record.partition(), record.offset(), k, payloadSize, 0));
        String seed = record.topic() + ":" + record.partition() + ":" + record.offset();
        Instant occurredAt = resolveOccurredAt(record);
        try {
            messageIngestRepository.upsertReadyMessage(
                    UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8))
                            .toString(),
                    k,
                    record.value(),
                    occurredAt);
            ack.acknowledge();
            observer.consumerSucceeded(new ConsumerObservation(
                    "blocking",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    k,
                    payloadSize,
                    elapsedMs(started)));
        } catch (RuntimeException e) {
            observer.consumerFailed(
                    new ConsumerObservation(
                            "blocking",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            k,
                            payloadSize,
                            elapsedMs(started)),
                    e);
            throw e;
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static Instant resolveOccurredAt(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(OCCURRED_AT_HEADER);
        if (header != null) {
            Instant parsed = parseOccurredAtHeader(header.value());
            if (parsed != null) {
                return parsed;
            }
        }
        long timestamp = record.timestamp();
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
