package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

class KafkaReactiveR2dbcConsumerServiceTest {

    @Test
    void onMessage_usesMetadataResolverAndAcks() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        ConsumerMetadataResolver metadataResolver = mock(ConsumerMetadataResolver.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                ingestRepository, NoopSuperduperObserver.INSTANCE, metadataResolver);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic-a", 2, 11L, "key1", "value1");

        when(metadataResolver.resolveMessageId("topic-a", 2, 11L, Map.of())).thenReturn("message-11");
        when(metadataResolver.resolveOccurredAt(-1L, Map.of())).thenReturn(Instant.parse("2026-02-21T10:15:30Z"));
        when(metadataResolver.resolveCorrelationId(Map.of())).thenReturn("corr-11");
        when(metadataResolver.resolveMessageType(Map.of())).thenReturn("order.created");
        when(ingestRepository.batchUpsertReadyMessages(any())).thenReturn(Mono.empty());

        svc.onMessage(List.of(record), ack);

        ArgumentCaptor<List<MessageIngestData>> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(ingestRepository).batchUpsertReadyMessages(batchCaptor.capture());
        assertThat(batchCaptor.getValue())
                .containsExactly(new MessageIngestData(
                        "default",
                        "message-11",
                        "key1",
                        "value1",
                        Instant.parse("2026-02-21T10:15:30Z"),
                        "corr-11",
                        "order.created"));
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_defaultsNullKeysWithinBatch() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        ConsumerMetadataResolver metadataResolver = mock(ConsumerMetadataResolver.class);
        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                ingestRepository, NoopSuperduperObserver.INSTANCE, metadataResolver);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> first = new ConsumerRecord<>("topic-a", 0, 7L, null, "value1");
        ConsumerRecord<String, String> second = new ConsumerRecord<>("topic-a", 0, 8L, "k2", "value2");

        when(metadataResolver.resolveMessageId(anyString(), anyInt(), anyLong(), any()))
                .thenReturn("message-7", "message-8");
        when(metadataResolver.resolveOccurredAt(anyLong(), any())).thenReturn(Instant.parse("2026-02-21T10:15:30Z"));
        when(metadataResolver.resolveCorrelationId(any())).thenReturn("corr-7", "corr-8");
        when(ingestRepository.batchUpsertReadyMessages(any())).thenReturn(Mono.empty());

        svc.onMessage(List.of(first, second), ack);

        ArgumentCaptor<List<MessageIngestData>> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(ingestRepository).batchUpsertReadyMessages(batchCaptor.capture());
        assertThat(batchCaptor.getValue())
                .extracting(MessageIngestData::messageKey)
                .containsExactly("default", "k2");
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_fallsBackToSingleRecordUpsertsWhenBatchFails() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc =
                new KafkaReactiveR2dbcConsumerService(ingestRepository, NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> first = new ConsumerRecord<>("t", 0, 9L, "key1", "value1");
        ConsumerRecord<String, String> second = new ConsumerRecord<>("t", 0, 10L, "key2", "value2");

        when(ingestRepository.batchUpsertReadyMessages(any()))
                .thenReturn(Mono.error(new RuntimeException("batch fail")));
        when(ingestRepository.upsertReadyMessage(
                        anyString(), anyString(), anyString(), anyString(), any(Instant.class), anyString(), isNull()))
                .thenReturn(Mono.empty());

        svc.onMessage(List.of(first, second), ack);

        verify(ingestRepository).batchUpsertReadyMessages(any());
        verify(ingestRepository)
                .upsertReadyMessage(
                        eq("default"),
                        anyString(),
                        eq("key1"),
                        eq("value1"),
                        any(Instant.class),
                        anyString(),
                        isNull());
        verify(ingestRepository)
                .upsertReadyMessage(
                        eq("default"),
                        anyString(),
                        eq("key2"),
                        eq("value2"),
                        any(Instant.class),
                        anyString(),
                        isNull());
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_whenSingleRecordFallbackFails_doesNotAck() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc =
                new KafkaReactiveR2dbcConsumerService(ingestRepository, NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        when(ingestRepository.batchUpsertReadyMessages(any()))
                .thenReturn(Mono.error(new RuntimeException("batch fail")));
        when(ingestRepository.upsertReadyMessage(
                        anyString(), anyString(), anyString(), anyString(), any(Instant.class), anyString(), isNull()))
                .thenReturn(Mono.error(new RuntimeException("single fail")));

        assertThatThrownBy(() -> svc.onMessage(List.of(new ConsumerRecord<>("t", 0, 0L, "k", "v")), ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("single fail");

        verify(ack, never()).acknowledge();
    }

    @Test
    void onMessage_withTopicRegistry_routesBatchesToCorrectRepositoryWithTopicValue() {
        ReactiveMessageIngestRepository repoA = mock(ReactiveMessageIngestRepository.class);
        ReactiveMessageIngestRepository repoB = mock(ReactiveMessageIngestRepository.class);
        TopicRepositoryFactory factory = mock(TopicRepositoryFactory.class);

        when(factory.createReactiveIngestRepository("orders_messages")).thenReturn(repoB);
        when(repoA.batchUpsertReadyMessages(any())).thenReturn(Mono.empty());
        when(repoB.batchUpsertReadyMessages(any())).thenReturn(Mono.empty());

        TopicConfigView topicA = topicConfig("topic-a", "kafka-topic-a", "handlerA", "");
        TopicConfigView topicB = topicConfig("topic-b", "kafka-topic-b", "handlerB", "orders_messages");
        TopicRegistryView topicRegistry = topicRegistry(topicA, topicB);

        KafkaReactiveR2dbcConsumerService svc = new KafkaReactiveR2dbcConsumerService(
                repoA, topicRegistry, factory, NoopSuperduperObserver.INSTANCE, null);

        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, String> recordA1 = new ConsumerRecord<>("kafka-topic-a", 0, 1L, "key-a1", "value-a1");
        ConsumerRecord<String, String> recordA2 = new ConsumerRecord<>("kafka-topic-a", 0, 2L, "key-a2", "value-a2");
        ConsumerRecord<String, String> recordB = new ConsumerRecord<>("kafka-topic-b", 0, 3L, "key-b", "value-b");

        svc.onMessage(List.of(recordA1, recordA2, recordB), ack);

        ArgumentCaptor<List<MessageIngestData>> batchA = ArgumentCaptor.forClass(List.class);
        verify(repoA).batchUpsertReadyMessages(batchA.capture());
        assertThat(batchA.getValue()).extracting(MessageIngestData::topic).containsOnly("kafka-topic-a");

        ArgumentCaptor<List<MessageIngestData>> batchB = ArgumentCaptor.forClass(List.class);
        verify(repoB).batchUpsertReadyMessages(batchB.capture());
        assertThat(batchB.getValue()).extracting(MessageIngestData::topic).containsOnly("kafka-topic-b");
        verify(ack).acknowledge();
    }

    @Test
    void onMessage_ignoresEmptyBatch() {
        ReactiveMessageIngestRepository ingestRepository = mock(ReactiveMessageIngestRepository.class);
        KafkaReactiveR2dbcConsumerService svc =
                new KafkaReactiveR2dbcConsumerService(ingestRepository, NoopSuperduperObserver.INSTANCE);
        Acknowledgment ack = mock(Acknowledgment.class);

        svc.onMessage(List.of(), ack);

        verify(ingestRepository, never()).batchUpsertReadyMessages(any());
        verify(ack, never()).acknowledge();
    }

    private static TopicConfigView topicConfig(String name, String kafkaTopic, String handlerBeanName, String table) {
        return new TopicConfigView() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String kafkaTopic() {
                return kafkaTopic;
            }

            @Override
            public String handlerBeanName() {
                return handlerBeanName;
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
                return table;
            }

            @Override
            public String claimLockName() {
                return "superduper-claim-" + name;
            }
        };
    }

    private static TopicRegistryView topicRegistry(TopicConfigView... topics) {
        List<TopicConfigView> topicList = List.of(topics);
        return new TopicRegistryView() {
            @Override
            public List<? extends TopicConfigView> topics() {
                return topicList;
            }

            @Override
            public List<String> kafkaTopics() {
                return topicList.stream().map(TopicConfigView::kafkaTopic).toList();
            }

            @Override
            public TopicConfigView getByKafkaTopic(String kafkaTopic) {
                return topicList.stream()
                        .filter(topic -> topic.kafkaTopic().equals(kafkaTopic))
                        .findFirst()
                        .orElseThrow();
            }
        };
    }
}
