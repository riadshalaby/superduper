package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

class KafkaConsumerServiceTest {

    @Test
    void onMessage_insertsAndAcks() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("t", 0, 0L, "key1", "value1");

        svc.onMessage(rec, ack);

        var uuidCap = ArgumentCaptor.forClass(String.class);
        var keyCap = ArgumentCaptor.forClass(String.class);
        var contentCap = ArgumentCaptor.forClass(String.class);
        var occurredAtCap = ArgumentCaptor.forClass(Instant.class);
        verify(ingestRepository)
                .upsertReadyMessage(uuidCap.capture(), keyCap.capture(), contentCap.capture(), occurredAtCap.capture());
        assertThat(keyCap.getValue()).isEqualTo("key1");
        assertThat(contentCap.getValue()).isEqualTo("value1");
        assertThat(uuidCap.getValue())
                .isEqualTo(UUID.nameUUIDFromBytes("t:0:0".getBytes(StandardCharsets.UTF_8))
                        .toString());
        assertThat(occurredAtCap.getValue()).isNotNull();
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_defaultsNullKey() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("t", 0, 7L, null, "value1");

        svc.onMessage(rec, ack);

        var keyCap = ArgumentCaptor.forClass(String.class);
        verify(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        keyCap.capture(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class));
        assertThat(keyCap.getValue()).isEqualTo("default");
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_whenRepositoryFails_doesNotAck() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("t", 0, 9L, "key1", "value1");

        doThrow(new RuntimeException("db fail"))
                .when(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(Instant.class));

        assertThatThrownBy(() -> svc.onMessage(rec, ack)).isInstanceOf(RuntimeException.class);
        verify(ack, never()).acknowledge();
    }

    @Test
    void onMessage_prefersOccurredAtHeader() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<>("t", 0, 11L, "key1", "value1");
        rec.headers().add("occurred_at", "2026-02-21T10:15:30Z".getBytes(StandardCharsets.UTF_8));

        svc.onMessage(rec, ack);

        var occurredAtCap = ArgumentCaptor.forClass(Instant.class);
        verify(ingestRepository)
                .upsertReadyMessage(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        occurredAtCap.capture());
        assertThat(occurredAtCap.getValue()).isEqualTo(Instant.parse("2026-02-21T10:15:30Z"));
    }

    @Test
    void onMessage_fallsBackToKafkaRecordTimestamp() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        long kafkaTimestamp = 1740132930000L;
        ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "t",
                0,
                12L,
                kafkaTimestamp,
                TimestampType.CREATE_TIME,
                0,
                0,
                "key1",
                "value1",
                new RecordHeaders(),
                Optional.empty());

        svc.onMessage(rec, ack);

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
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "t",
                0,
                13L,
                -1L,
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                "key1",
                "value1",
                new RecordHeaders(),
                Optional.empty());
        rec.headers().add("occurred_at", "invalid-ts".getBytes(StandardCharsets.UTF_8));

        Instant before = Instant.now();
        svc.onMessage(rec, ack);
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
