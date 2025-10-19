package net.rsworld.superduper.consumer.reactive;

import jakarta.annotation.PostConstruct;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

@Service
class KafkaReactorR2dbcConsumerService {
    private final KafkaReceiver<String, String> receiver;
    private final DatabaseClient db;

    KafkaReactorR2dbcConsumerService(KafkaReceiver<String, String> receiver, DatabaseClient db) {
        this.receiver = receiver;
        this.db = db;
    }

    @PostConstruct
    public void start() {
        receiver.receive()
                .concatMap(rec -> db.sql(
                                "INSERT INTO messages (uuid, key, content, status, timestamp, retry_count, last_updated) "
                                        + "VALUES (:uuid, :key, :content, 'READY', NOW(), 0, NOW()) "
                                        + "ON CONFLICT (uuid) DO UPDATE SET last_updated = EXCLUDED.last_updated")
                        .bind("uuid", UUID.randomUUID().toString())
                        .bind("key", rec.key())
                        .bind("content", rec.value())
                        .fetch()
                        .rowsUpdated()
                        .doOnSuccess(x -> rec.receiverOffset().acknowledge()))
                .subscribe();
    }
}
