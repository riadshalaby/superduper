package net.rsworld.superduper.consumer.reactive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestData;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class KafkaReactiveR2dbcConsumerService {
    private static final String MODE_REACTIVE = "reactive";
    private static final String DEFAULT_GROUP_KEY = "__default__";

    private final ReactiveMessageIngestRepository defaultRepository;
    private final Map<String, ReactiveMessageIngestRepository> repositoriesByTopic;
    private final TopicRegistryView topicRegistry;
    private final SuperduperObserver observer;
    private final ConsumerMetadataResolver metadataResolver;

    KafkaReactiveR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository,
            TopicRegistryView topicRegistry,
            TopicRepositoryFactory repositoryFactory,
            SuperduperObserver observer,
            ConsumerMetadataResolver metadataResolver) {
        this.defaultRepository = messageIngestRepository;
        this.topicRegistry = topicRegistry;
        this.repositoriesByTopic = topicRegistry == null || repositoryFactory == null
                ? Map.of()
                : buildRepositories(topicRegistry, messageIngestRepository, repositoryFactory);
        this.observer = observer;
        this.metadataResolver = metadataResolver == null ? new DefaultConsumerMetadataResolver() : metadataResolver;
    }

    KafkaReactiveR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this(messageIngestRepository, null, null, observer, null);
    }

    KafkaReactiveR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository,
            SuperduperObserver observer,
            ConsumerMetadataResolver metadataResolver) {
        this(messageIngestRepository, null, null, observer, metadataResolver);
    }

    @KafkaListener(topics = "#{@superduperKafkaTopics}", containerFactory = "reactiveKafkaListenerContainerFactory")
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment ack) {
        if (consumerRecords == null || consumerRecords.isEmpty()) {
            return;
        }

        List<IngestRecord> records = new ArrayList<>(consumerRecords.size());
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            records.add(toIngestRecord(consumerRecord));
        }

        Map<String, List<IngestRecord>> recordsByGroup = new LinkedHashMap<>();
        for (IngestRecord r : records) {
            recordsByGroup
                    .computeIfAbsent(r.groupKey(), ignored -> new ArrayList<>())
                    .add(r);
        }

        for (List<IngestRecord> groupRecords : recordsByGroup.values()) {
            IngestRecord sample = groupRecords.getFirst();
            List<MessageIngestData> batchMessages =
                    groupRecords.stream().map(IngestRecord::message).toList();
            sample.repository()
                    .batchUpsertReadyMessages(batchMessages)
                    .onErrorResume(_ -> persistIndividually(groupRecords))
                    .block();
        }

        ack.acknowledge();
        for (IngestRecord r : records) {
            observer.consumerSucceeded(r.observation(elapsedMs(r.startedNanos())));
        }
    }

    private IngestRecord toIngestRecord(ConsumerRecord<String, String> consumerRecord) {
        long started = System.nanoTime();
        String kafkaTopic = consumerRecord.topic();
        String messageKey = consumerRecord.key() != null ? consumerRecord.key() : "default";
        int payloadSize = consumerRecord.value() == null
                ? 0
                : consumerRecord.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        observer.consumerReceived(new ConsumerObservation(
                MODE_REACTIVE,
                kafkaTopic,
                consumerRecord.partition(),
                consumerRecord.offset(),
                messageKey,
                payloadSize,
                0));

        Map<String, byte[]> headers = toHeaderMap(consumerRecord);
        String messageId = metadataResolver.resolveMessageId(
                kafkaTopic, consumerRecord.partition(), consumerRecord.offset(), headers);
        Instant occurredAt = metadataResolver.resolveOccurredAt(consumerRecord.timestamp(), headers);
        String correlationId = metadataResolver.resolveCorrelationId(headers);
        String messageType = metadataResolver.resolveMessageType(headers);

        if (topicRegistry == null) {
            return new IngestRecord(
                    DEFAULT_GROUP_KEY,
                    defaultRepository,
                    new MessageIngestData(
                            "default",
                            messageId,
                            messageKey,
                            consumerRecord.value(),
                            occurredAt,
                            correlationId,
                            messageType),
                    kafkaTopic,
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    messageKey,
                    payloadSize,
                    started);
        }

        return new IngestRecord(
                kafkaTopic,
                repositoriesByTopic.get(kafkaTopic),
                new MessageIngestData(
                        topicRegistry.getByKafkaTopic(kafkaTopic).topicColumnValue(),
                        messageId,
                        messageKey,
                        consumerRecord.value(),
                        occurredAt,
                        correlationId,
                        messageType),
                kafkaTopic,
                consumerRecord.partition(),
                consumerRecord.offset(),
                messageKey,
                payloadSize,
                started);
    }

    private Mono<Void> persistIndividually(List<IngestRecord> records) {
        return Flux.fromIterable(records)
                .concatMap(r -> {
                    MessageIngestData message = r.message();
                    return r.repository()
                            .upsertReadyMessage(
                                    message.topic(),
                                    message.messageId(),
                                    message.messageKey(),
                                    message.content(),
                                    message.occurredAt(),
                                    message.correlationId(),
                                    message.messageType())
                            .onErrorResume(e -> {
                                observer.consumerFailed(r.observation(elapsedMs(r.startedNanos())), e);
                                return Mono.error(e);
                            });
                })
                .then();
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static Map<String, byte[]> toHeaderMap(ConsumerRecord<String, String> consumerRecord) {
        Map<String, byte[]> headers = new HashMap<>();
        consumerRecord.headers().forEach(header -> headers.put(header.key(), header.value()));
        return headers;
    }

    private static Map<String, ReactiveMessageIngestRepository> buildRepositories(
            TopicRegistryView topicRegistry,
            ReactiveMessageIngestRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        Map<String, ReactiveMessageIngestRepository> repositories = new HashMap<>();
        for (var topic : topicRegistry.topics()) {
            String kafkaTopic = topic.kafkaTopic();
            if (kafkaTopic == null || kafkaTopic.isBlank()) {
                continue;
            }
            repositories.put(
                    kafkaTopic,
                    topic.table().isBlank()
                            ? sharedRepository
                            : repositoryFactory.createReactiveIngestRepository(topic.table()));
        }
        return repositories;
    }

    private record IngestRecord(
            String groupKey,
            ReactiveMessageIngestRepository repository,
            MessageIngestData message,
            String kafkaTopic,
            int partition,
            long offset,
            String messageKey,
            int payloadSize,
            long startedNanos) {
        ConsumerObservation observation(long durationMs) {
            return new ConsumerObservation(
                    MODE_REACTIVE, kafkaTopic, partition, offset, messageKey, payloadSize, durationMs);
        }
    }
}
