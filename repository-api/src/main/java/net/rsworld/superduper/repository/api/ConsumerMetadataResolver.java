package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.Map;

/**
 * Resolves library-level metadata fields from an incoming consumer record.
 *
 * <p>Consumer implementations call this extension point to derive stable identifiers and timestamps before persisting a
 * message as {@code READY}.
 */
public interface ConsumerMetadataResolver {
    /**
     * Resolves the logical message id for an incoming record.
     *
     * @param topic the Kafka topic that produced the record
     * @param partition the Kafka partition that produced the record
     * @param offset the Kafka offset of the record
     * @param headers the decoded record headers keyed by header name
     * @return the message id to persist
     */
    String resolveMessageId(String topic, int partition, long offset, Map<String, byte[]> headers);

    /**
     * Resolves the business occurrence timestamp for an incoming record.
     *
     * @param recordTimestamp the broker-provided record timestamp in epoch milliseconds
     * @param headers the decoded record headers keyed by header name
     * @return the timestamp to persist with the message
     */
    Instant resolveOccurredAt(long recordTimestamp, Map<String, byte[]> headers);

    /**
     * Resolves the correlation id for an incoming record.
     *
     * @param headers the decoded record headers keyed by header name
     * @return the correlation id to persist
     */
    String resolveCorrelationId(Map<String, byte[]> headers);

    /**
     * Resolves the message type for an incoming record.
     *
     * @param headers the decoded record headers keyed by header name
     * @return the message type to persist
     */
    String resolveMessageType(Map<String, byte[]> headers);
}
