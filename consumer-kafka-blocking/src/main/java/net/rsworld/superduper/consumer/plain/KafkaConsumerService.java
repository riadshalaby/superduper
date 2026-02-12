package net.rsworld.superduper.consumer.plain;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
class KafkaConsumerService {
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
        observer.consumerReceived(
                new ConsumerObservation("blocking", record.topic(), record.partition(), record.offset(), k, payloadSize, 0));
        String seed = record.topic() + ":" + record.partition() + ":" + record.offset();
        try {
            messageIngestRepository.upsertReadyMessage(
                    UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString(), k, record.value());
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
}
