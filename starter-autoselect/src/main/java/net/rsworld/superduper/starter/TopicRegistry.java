package net.rsworld.superduper.starter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;

public class TopicRegistry implements TopicRegistryView {
    private final List<ResolvedTopicConfig> topics;
    private final Map<String, ResolvedTopicConfig> topicsByKafkaTopic;

    public TopicRegistry(List<ResolvedTopicConfig> topics) {
        this.topics = List.copyOf(topics);
        this.topicsByKafkaTopic = new LinkedHashMap<>();
        for (ResolvedTopicConfig topic : topics) {
            ResolvedTopicConfig existing = topicsByKafkaTopic.put(topic.kafkaTopic(), topic);
            if (existing != null) {
                throw new IllegalArgumentException("Duplicate Kafka topic configured: " + topic.kafkaTopic());
            }
        }
    }

    @Override
    public List<ResolvedTopicConfig> topics() {
        return topics;
    }

    @Override
    public List<String> kafkaTopics() {
        return topics.stream().map(ResolvedTopicConfig::kafkaTopic).toList();
    }

    @Override
    public TopicConfigView getByKafkaTopic(String kafkaTopic) {
        ResolvedTopicConfig resolved = topicsByKafkaTopic.get(kafkaTopic);
        if (resolved == null) {
            throw new IllegalArgumentException("No topic configuration found for Kafka topic '" + kafkaTopic + "'");
        }
        return resolved;
    }

    public record ResolvedTopicConfig(
            String name,
            String kafkaTopic,
            String handlerBeanName,
            int batchSize,
            int maxRetries,
            String table,
            String claimLockName)
            implements TopicConfigView {}
}
