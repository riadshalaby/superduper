package net.rsworld.superduper.consumer.plain;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
class KafkaConsumerService {
    private static final String MODE_BLOCKING = "blocking";

    private final MessageIngestRepository messageIngestRepository;
    private final SuperduperObserver observer;
    private final ConsumerMetadataResolver metadataResolver;

    KafkaConsumerService(MessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this(messageIngestRepository, observer, new DefaultConsumerMetadataResolver());
    }

    KafkaConsumerService(
            MessageIngestRepository messageIngestRepository,
            SuperduperObserver observer,
            ConsumerMetadataResolver metadataResolver) {
        this.messageIngestRepository = messageIngestRepository;
        this.observer = observer;
        this.metadataResolver = metadataResolver == null ? new DefaultConsumerMetadataResolver() : metadataResolver;
    }

    @KafkaListener(
            topics = "#{'${superduper.kafka.topics:${superduper.kafka.topic}}'.split('\\\\s*,\\\\s*')}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        long started = System.nanoTime();
        String messageKey = consumerRecord.key() != null ? consumerRecord.key() : "default";
        int payloadSize = consumerRecord.value() == null
                ? 0
                : consumerRecord.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        observer.consumerReceived(new ConsumerObservation(
                MODE_BLOCKING,
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                messageKey,
                payloadSize,
                0));
        Map<String, byte[]> headers = toHeaderMap(consumerRecord);
        String messageId = metadataResolver.resolveMessageId(
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), headers);
        Instant occurredAt = metadataResolver.resolveOccurredAt(consumerRecord.timestamp(), headers);
        String correlationId = metadataResolver.resolveCorrelationId(headers);
        String messageType = metadataResolver.resolveMessageType(headers);
        try {
            messageIngestRepository.upsertReadyMessage(
                    messageId, messageKey, consumerRecord.value(), occurredAt, correlationId, messageType);
            ack.acknowledge();
            observer.consumerSucceeded(new ConsumerObservation(
                    MODE_BLOCKING,
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    messageKey,
                    payloadSize,
                    elapsedMs(started)));
        } catch (RuntimeException e) {
            observer.consumerFailed(
                    new ConsumerObservation(
                            MODE_BLOCKING,
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            messageKey,
                            payloadSize,
                            elapsedMs(started)),
                    e);
            throw e;
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static Map<String, byte[]> toHeaderMap(ConsumerRecord<String, String> consumerRecord) {
        Map<String, byte[]> headers = new HashMap<>();
        consumerRecord.headers().forEach(header -> headers.put(header.key(), header.value()));
        return headers;
    }
}
