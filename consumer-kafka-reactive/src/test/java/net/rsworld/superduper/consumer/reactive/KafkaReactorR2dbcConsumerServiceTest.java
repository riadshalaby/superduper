package net.rsworld.superduper.consumer.reactive;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

class KafkaReactorR2dbcConsumerServiceTest {

    @Test
    void onMessage_success_acknowledges() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactorR2dbcConsumerService svc = new KafkaReactorR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.empty());

        svc.onMessage(new ConsumerRecord<>("t", 0, 0L, "k", "v"), ack);

        verify(ack).acknowledge();
    }

    @Test
    void onMessage_whenRepositoryFails_doesNotAck() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactorR2dbcConsumerService svc = new KafkaReactorR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.error(new RuntimeException("db fail")));

        svc.onMessage(new ConsumerRecord<>("t", 0, 0L, "k", "v"), ack);

        verify(ack, never()).acknowledge();
    }

    @Test
    void onMessage_prefersOccurredAtHeader() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactorR2dbcConsumerService svc = new KafkaReactorR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.empty());

        ConsumerRecord<String, String> record = new ConsumerRecord<>("t", 0, 13L, "k", "v");
        record.headers().add("occurred_at", "1740132930000".getBytes(StandardCharsets.UTF_8));
        svc.onMessage(record, ack);

        verify(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.eq(Instant.ofEpochMilli(1740132930000L)));
    }
}
