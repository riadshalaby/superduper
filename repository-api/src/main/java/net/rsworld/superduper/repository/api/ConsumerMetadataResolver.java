package net.rsworld.superduper.repository.api;

import java.time.Instant;
import java.util.Map;

public interface ConsumerMetadataResolver {
    String resolveMessageId(String topic, int partition, long offset, Map<String, byte[]> headers);

    Instant resolveOccurredAt(long recordTimestamp, Map<String, byte[]> headers);

    String resolveCorrelationId(Map<String, byte[]> headers);

    String resolveMessageType(Map<String, byte[]> headers);
}
