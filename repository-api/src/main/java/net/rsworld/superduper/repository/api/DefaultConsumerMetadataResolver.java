package net.rsworld.superduper.repository.api;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.UUID;

public class DefaultConsumerMetadataResolver implements ConsumerMetadataResolver {
    private static final String MESSAGE_ID_HEADER = "message_id";
    private static final String OCCURRED_AT_HEADER = "occurred_at";
    private static final String CORRELATION_ID_HEADER = "correlationId";
    private static final String MESSAGE_TYPE_HEADER = "message_type";

    @Override
    public String resolveMessageId(String topic, int partition, long offset, Map<String, byte[]> headers) {
        String headerValue = readHeader(headers, MESSAGE_ID_HEADER);
        if (headerValue != null) {
            return headerValue;
        }
        String seed = topic + ":" + partition + ":" + offset;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    @Override
    public Instant resolveOccurredAt(long recordTimestamp, Map<String, byte[]> headers) {
        String headerValue = readHeader(headers, OCCURRED_AT_HEADER);
        if (headerValue != null) {
            Instant parsed = parseInstant(headerValue);
            if (parsed != null) {
                return parsed;
            }
        }
        if (recordTimestamp > 0) {
            return Instant.ofEpochMilli(recordTimestamp);
        }
        return Instant.now();
    }

    @Override
    public String resolveCorrelationId(Map<String, byte[]> headers) {
        String headerValue = readHeader(headers, CORRELATION_ID_HEADER);
        if (headerValue != null) {
            return headerValue;
        }
        return UUID.randomUUID().toString();
    }

    @Override
    public String resolveMessageType(Map<String, byte[]> headers) {
        return readHeader(headers, MESSAGE_TYPE_HEADER);
    }

    private static String readHeader(Map<String, byte[]> headers, String name) {
        if (headers == null) {
            return null;
        }
        byte[] raw = headers.get(name);
        if (raw == null || raw.length == 0) {
            return null;
        }
        String value = new String(raw, StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? null : value;
    }

    private static Instant parseInstant(String raw) {
        try {
            return Instant.ofEpochMilli(Long.parseLong(raw));
        } catch (NumberFormatException ignored) {
            // Continue with ISO-8601 parsing.
        }
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
