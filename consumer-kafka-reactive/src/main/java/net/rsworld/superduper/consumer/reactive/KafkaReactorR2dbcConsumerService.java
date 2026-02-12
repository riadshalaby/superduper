package net.rsworld.superduper.consumer.reactive;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

class KafkaReactorR2dbcConsumerService {
    private final ReactiveMessageIngestRepository messageIngestRepository;
    private final SuperduperObserver observer;

    KafkaReactorR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this.messageIngestRepository = messageIngestRepository;
        this.observer = observer;
    }

    @KafkaListener(topics = "${superduper.kafka.topic}", containerFactory = "reactiveKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        long started = System.nanoTime();
        String key = record.key() != null ? record.key() : "default";
        int payloadSize = record.value() == null ? 0 : record.value().getBytes(StandardCharsets.UTF_8).length;
        observer.consumerReceived(
                new ConsumerObservation("reactive", record.topic(), record.partition(), record.offset(), key, payloadSize, 0));
        String seed = record.topic() + ":" + record.partition() + ":" + record.offset();
        messageIngestRepository
                .upsertReadyMessage(
                        UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8))
                                .toString(),
                        key,
                        record.value())
                .doOnSuccess(x -> {
                    ack.acknowledge();
                    observer.consumerSucceeded(new ConsumerObservation(
                            "reactive",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            key,
                            payloadSize,
                            elapsedMs(started)));
                })
                .onErrorResume(e -> {
                    observer.consumerFailed(
                            new ConsumerObservation(
                                    "reactive",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
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
}
