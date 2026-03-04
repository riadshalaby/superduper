package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

class KafkaReactiveR2dbcConsumerServiceTest {

    @Test
    void onMessage_success_acknowledges() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
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
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.error(new RuntimeException("db fail")));

        assertThatThrownBy(() -> svc.onMessage(new ConsumerRecord<>("t", 0, 0L, "k", "v"), ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("db fail");

        verify(ack, never()).acknowledge();
    }

    @Test
    void onMessage_prefersOccurredAtHeader() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
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

    @Test
    void onMessage_fallsBackToKafkaRecordTimestamp() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.empty());

        long kafkaTimestamp = 1740132930000L;
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "t",
                0,
                14L,
                kafkaTimestamp,
                TimestampType.CREATE_TIME,
                0,
                0,
                "k",
                "v",
                new RecordHeaders(),
                Optional.empty());

        svc.onMessage(record, ack);

        var occurredAtCap = ArgumentCaptor.forClass(Instant.class);
        verify(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        occurredAtCap.capture());
        assertThat(occurredAtCap.getValue()).isEqualTo(Instant.ofEpochMilli(kafkaTimestamp));
    }

    @Test
    void onMessage_fallsBackToNow_whenHeaderAndKafkaTimestampInvalid() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class)))
                .thenReturn(Mono.empty());

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "t",
                0,
                15L,
                -1L,
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                "k",
                "v",
                new RecordHeaders(),
                Optional.empty());
        record.headers().add("occurred_at", "invalid-ts".getBytes(StandardCharsets.UTF_8));

        Instant before = Instant.now();
        svc.onMessage(record, ack);
        Instant after = Instant.now();

        var occurredAtCap = ArgumentCaptor.forClass(Instant.class);
        verify(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        occurredAtCap.capture());
        assertThat(occurredAtCap.getValue()).isBetween(before, after);
    }
}
