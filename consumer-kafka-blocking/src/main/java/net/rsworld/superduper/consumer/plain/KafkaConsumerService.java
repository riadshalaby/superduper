package net.rsworld.superduper.consumer.plain;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.ConsumerObservation;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

class KafkaConsumerService {
    private static final String MODE_BLOCKING = "blocking";

    private final MessageIngestRepository defaultRepository;
    private final Map<String, MessageIngestRepository> repositoriesByTopic;
    private final TopicRegistryView topicRegistry;
    private final SuperduperObserver observer;
    private final ConsumerMetadataResolver metadataResolver;

    KafkaConsumerService(
            MessageIngestRepository messageIngestRepository,
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

    KafkaConsumerService(MessageIngestRepository messageIngestRepository, SuperduperObserver observer) {
        this(messageIngestRepository, null, null, observer, null);
    }

    KafkaConsumerService(
            MessageIngestRepository messageIngestRepository,
            SuperduperObserver observer,
            ConsumerMetadataResolver metadataResolver) {
        this(messageIngestRepository, null, null, observer, metadataResolver);
    }

    @KafkaListener(topics = "#{@superduperKafkaTopics}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
        long started = System.nanoTime();
        String kafkaTopic = consumerRecord.topic();
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
            if (topicRegistry == null) {
                defaultRepository.upsertReadyMessage(
                        messageId, messageKey, consumerRecord.value(), occurredAt, correlationId, messageType);
            } else {
                var topicConfig = topicRegistry.getByKafkaTopic(kafkaTopic);
                MessageIngestRepository messageIngestRepository = repositoriesByTopic.get(kafkaTopic);
                messageIngestRepository.upsertReadyMessage(
                        topicConfig.kafkaTopic(),
                        messageId,
                        messageKey,
                        consumerRecord.value(),
                        occurredAt,
                        correlationId,
                        messageType);
            }
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

    private static Map<String, MessageIngestRepository> buildRepositories(
            TopicRegistryView topicRegistry,
            MessageIngestRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        Map<String, MessageIngestRepository> repositories = new HashMap<>();
        for (var topic : topicRegistry.topics()) {
            repositories.put(
                    topic.kafkaTopic(),
                    topic.table().isBlank()
                            ? sharedRepository
                            : repositoryFactory.createIngestRepository(topic.table()));
        }
        return repositories;
    }
}
