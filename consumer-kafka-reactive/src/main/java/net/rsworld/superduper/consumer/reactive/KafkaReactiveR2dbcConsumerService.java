package net.rsworld.superduper.consumer.reactive;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

class KafkaReactiveR2dbcConsumerService {
    private static final String MODE_REACTIVE = "reactive";

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
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        long started = System.nanoTime();
        String kafkaTopic = consumerRecord.topic();
        String messageKey = consumerRecord.key() != null ? consumerRecord.key() : "default";
        int payloadSize = consumerRecord.value() == null
                ? 0
                : consumerRecord.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        observer.consumerReceived(new ConsumerObservation(
                MODE_REACTIVE,
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
        var upsert = topicRegistry == null
                ? defaultRepository.upsertReadyMessage(
                        messageId, messageKey, consumerRecord.value(), occurredAt, correlationId, messageType)
                : repositoriesByTopic
                        .get(kafkaTopic)
                        .upsertReadyMessage(
                                topicRegistry.getByKafkaTopic(kafkaTopic).kafkaTopic(),
                                messageId,
                                messageKey,
                                consumerRecord.value(),
                                occurredAt,
                                correlationId,
                                messageType);
        upsert.doOnSuccess(unused -> {
                    ack.acknowledge();
                    observer.consumerSucceeded(new ConsumerObservation(
                            MODE_REACTIVE,
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            messageKey,
                            payloadSize,
                            elapsedMs(started)));
                })
                .doOnError(e -> observer.consumerFailed(
                        new ConsumerObservation(
                                MODE_REACTIVE,
                                consumerRecord.topic(),
                                consumerRecord.partition(),
                                consumerRecord.offset(),
                                messageKey,
                                payloadSize,
                                elapsedMs(started)),
                        e))
                .block();
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
            repositories.put(
                    topic.kafkaTopic(),
                    topic.table().isBlank()
                            ? sharedRepository
                            : repositoryFactory.createReactiveIngestRepository(topic.table()));
        }
        return repositories;
    }
}
