package net.rsworld.superduper.repository.api;

/**
 * Read-only view of one logical topic configuration.
 *
 * <p>Starter and worker infrastructure use this projection to expose resolved topic settings without leaking mutable
 * configuration objects.
 */
public interface TopicConfigView {
    /**
     * Returns the logical topic name from configuration.
     *
     * @return the logical topic name
     */
    String name();

    /**
     * Returns the Kafka topic name that messages are consumed from.
     *
     * @return the Kafka topic name
     */
    String kafkaTopic();

    /**
     * Returns the value stored in the message {@code topic} column.
     *
     * <p>Kafka-backed topics keep using their Kafka topic name. Configurations without a Kafka source fall back to
     * their logical name so workers can still claim rows consistently.
     *
     * @return the topic column value to persist and query
     */
    default String topicColumnValue() {
        String kafka = kafkaTopic();
        return kafka != null && !kafka.isBlank() ? kafka : name();
    }

    /**
     * Returns the Spring bean name of the handler responsible for this topic.
     *
     * @return the message handler bean name
     */
    String handlerBeanName();

    /**
     * Returns the maximum number of rows workers should claim at once.
     *
     * @return the configured batch size
     */
    int batchSize();

    /**
     * Returns the maximum retry count before messages transition to {@code STOPPED}.
     *
     * @return the configured retry limit
     */
    int maxRetries();

    /**
     * Returns the dedicated message table name, or a blank string when the shared table is used.
     *
     * @return the table name configuration
     */
    String table();

    /**
     * Returns the lock name used when coordinating scheduled claim loops for this topic.
     *
     * @return the claim lock name
     */
    String claimLockName();
}
