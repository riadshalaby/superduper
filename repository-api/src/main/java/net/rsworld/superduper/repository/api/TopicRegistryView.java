package net.rsworld.superduper.repository.api;

import java.util.List;

/** Exposes the configured topic mappings used by repository, consumer, and worker services. */
public interface TopicRegistryView {
    /**
     * Returns all configured topic entries.
     *
     * @return the configured topics in registry order
     */
    List<? extends TopicConfigView> topics();

    /**
     * Returns the Kafka topic names known to this registry.
     *
     * @return the configured Kafka topic names
     */
    List<String> kafkaTopics();

    /**
     * Looks up the topic configuration for a Kafka topic.
     *
     * @param kafkaTopic the Kafka topic name to resolve
     * @return the matching topic configuration
     */
    TopicConfigView getByKafkaTopic(String kafkaTopic);
}
