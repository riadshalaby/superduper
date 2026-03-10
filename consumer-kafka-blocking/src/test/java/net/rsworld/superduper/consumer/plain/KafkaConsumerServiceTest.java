package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

class KafkaConsumerServiceTest {

    @Test
    void onMessage_usesMetadataResolverAndAcks() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        ConsumerMetadataResolver metadataResolver = mock(ConsumerMetadataResolver.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                metadataResolver);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic-a", 2, 11L, "key1", "value1");

        when(metadataResolver.resolveMessageId("topic-a", 2, 11L, Map.of())).thenReturn("message-11");
        when(metadataResolver.resolveOccurredAt(-1L, Map.of())).thenReturn(Instant.parse("2026-02-21T10:15:30Z"));
        when(metadataResolver.resolveCorrelationId(Map.of())).thenReturn("corr-11");
        when(metadataResolver.resolveMessageType(Map.of())).thenReturn("order.created");

        svc.onMessage(record, ack);

        verify(ingestRepository)
                .upsertReadyMessage(
                        "message-11",
                        "key1",
                        "value1",
                        Instant.parse("2026-02-21T10:15:30Z"),
                        "corr-11",
                        "order.created");
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_defaultsNullKey() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        ConsumerMetadataResolver metadataResolver = mock(ConsumerMetadataResolver.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository,
                net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE,
                metadataResolver);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic-a", 0, 7L, null, "value1");

        when(metadataResolver.resolveMessageId(anyString(), anyInt(), anyLong(), any()))
                .thenReturn("message-7");
        when(metadataResolver.resolveOccurredAt(anyLong(), any())).thenReturn(Instant.parse("2026-02-21T10:15:30Z"));
        when(metadataResolver.resolveCorrelationId(any())).thenReturn("corr-7");

        svc.onMessage(record, ack);

        verify(ingestRepository)
                .upsertReadyMessage(
                        "message-7", "default", "value1", Instant.parse("2026-02-21T10:15:30Z"), "corr-7", null);
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_whenRepositoryFails_doesNotAck() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t", 0, 9L, "key1", "value1");

        doThrow(new RuntimeException("db fail"))
                .when(ingestRepository)
                .upsertReadyMessage(anyString(), anyString(), anyString(), any(Instant.class), anyString(), isNull());

        assertThatThrownBy(() -> svc.onMessage(record, ack)).isInstanceOf(RuntimeException.class);
        verify(ack, never()).acknowledge();
    }

    @Test
    void onMessage_defaultResolverFlowsOccurredAtAndCorrelationMetadata() {
        MessageIngestRepository ingestRepository = mock(MessageIngestRepository.class);
        KafkaConsumerService svc = new KafkaConsumerService(
                ingestRepository, net.rsworld.superduper.observability.api.NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t", 0, 11L, "key1", "value1");
        record.headers().add("occurred_at", "2026-02-21T10:15:30Z".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.headers().add("correlationId", "corr-11".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.headers().add("message_type", "invoice.created".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        svc.onMessage(record, ack);

        ArgumentCaptor<Instant> occurredAt = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<String> correlationId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageType = ArgumentCaptor.forClass(String.class);
        verify(ingestRepository)
                .upsertReadyMessage(
                        anyString(),
                        anyString(),
                        anyString(),
                        occurredAt.capture(),
                        correlationId.capture(),
                        messageType.capture());
        assertThat(occurredAt.getValue()).isEqualTo(Instant.parse("2026-02-21T10:15:30Z"));
        assertThat(correlationId.getValue()).isEqualTo("corr-11");
        assertThat(messageType.getValue()).isEqualTo("invoice.created");
    }

    @Test
    void onMessage_withTopicRegistry_routesToCorrectRepositoryWithTopicValue() {
        MessageIngestRepository repoA = mock(MessageIngestRepository.class);
        MessageIngestRepository repoB = mock(MessageIngestRepository.class);

        TopicRepositoryFactory factory = mock(TopicRepositoryFactory.class);
        when(factory.createIngestRepository("orders_messages")).thenReturn(repoB);

        TopicConfigView topicA = new TopicConfigView() {
            @Override
            public String name() {
                return "topic-a";
            }

            @Override
            public String kafkaTopic() {
                return "kafka-topic-a";
            }

            @Override
            public String handlerBeanName() {
                return "handlerA";
            }

            @Override
            public int batchSize() {
                return 10;
            }

            @Override
            public int maxRetries() {
                return 3;
            }

            @Override
            public String table() {
                return "";
            }

            @Override
            public String claimLockName() {
                return "superduper-claim-kafka-topic-a";
            }
        };

        TopicConfigView topicB = new TopicConfigView() {
            @Override
            public String name() {
                return "topic-b";
            }

            @Override
            public String kafkaTopic() {
                return "kafka-topic-b";
            }

            @Override
            public String handlerBeanName() {
                return "handlerB";
            }

            @Override
            public int batchSize() {
                return 10;
            }

            @Override
            public int maxRetries() {
                return 3;
            }

            @Override
            public String table() {
                return "orders_messages";
            }

            @Override
            public String claimLockName() {
                return "superduper-claim-kafka-topic-b";
            }
        };

        List<TopicConfigView> topics = List.of(topicA, topicB);
        TopicRegistryView topicRegistry = new TopicRegistryView() {
            @Override
            public List<? extends TopicConfigView> topics() {
                return topics;
            }

            @Override
            public List<String> kafkaTopics() {
                return topics.stream().map(TopicConfigView::kafkaTopic).toList();
            }

            @Override
            public TopicConfigView getByKafkaTopic(String kafkaTopic) {
                return topics.stream()
                        .filter(t -> t.kafkaTopic().equals(kafkaTopic))
                        .findFirst()
                        .orElseThrow();
            }
        };

        KafkaConsumerService svc =
                new KafkaConsumerService(repoA, topicRegistry, factory, NoopSuperduperObserver.INSTANCE, null);

        Acknowledgment ackA = mock(Acknowledgment.class);
        ConsumerRecord<String, String> recordA = new ConsumerRecord<>("kafka-topic-a", 0, 1L, "key-a", "value-a");
        svc.onMessage(recordA, ackA);

        Acknowledgment ackB = mock(Acknowledgment.class);
        ConsumerRecord<String, String> recordB = new ConsumerRecord<>("kafka-topic-b", 0, 2L, "key-b", "value-b");
        svc.onMessage(recordB, ackB);

        verify(repoA)
                .upsertReadyMessage(
                        eq("kafka-topic-a"),
                        anyString(),
                        eq("key-a"),
                        eq("value-a"),
                        any(Instant.class),
                        anyString(),
                        isNull());
        verify(repoB)
                .upsertReadyMessage(
                        eq("kafka-topic-b"),
                        anyString(),
                        eq("key-b"),
                        eq("value-b"),
                        any(Instant.class),
                        anyString(),
                        isNull());
        verify(ackA).acknowledge();
        verify(ackB).acknowledge();
    }
}
