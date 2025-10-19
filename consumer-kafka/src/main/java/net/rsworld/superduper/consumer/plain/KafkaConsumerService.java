package net.rsworld.superduper.consumer.plain;

import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
class KafkaConsumerService {
    private final NamedParameterJdbcTemplate jdbc;

    KafkaConsumerService(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Value("${superduper.kafka.topic}")
    String topic;

    @KafkaListener(topics = "#{__listener.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String k = record.key() != null ? record.key() : "default";
        String sql = "INSERT INTO messages (uuid, key, content, status, timestamp, retry_count, last_updated) "
                + "VALUES (:uuid, :key, :content, 'READY', now(), 0, now()) "
                + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated";
        jdbc.update(
                sql,
                new MapSqlParameterSource()
                        .addValue("uuid", UUID.randomUUID().toString())
                        .addValue("key", k)
                        .addValue("content", record.value()));
        ack.acknowledge();
    }
}
