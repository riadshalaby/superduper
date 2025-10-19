package net.rsworld.superduper.example.reactive;

import net.rsworld.superduper.worker.reactive.MessageRow;
import net.rsworld.superduper.worker.reactive.ProcessingResult;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ReactiveExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveExampleApplication.class, args);
    }

    // Schema-Init für R2DBC
    @Bean
    ApplicationRunner initSchema(DatabaseClient db) {
        return args -> db.sql("""
      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        uuid VARCHAR(36) UNIQUE NOT NULL,
        key VARCHAR(255) NOT NULL,
        content TEXT,
        status TEXT NOT NULL,
        retry_count INT DEFAULT 0,
        container_id VARCHAR(255),
        timestamp TIMESTAMP,
        last_updated TIMESTAMP DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_messages_key_id ON messages(key, id);
      CREATE TABLE IF NOT EXISTS container_heartbeats (
        container_id VARCHAR(255) PRIMARY KEY,
        last_heartbeat TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS shedlock (
        name VARCHAR(64) PRIMARY KEY,
        lock_until TIMESTAMP(3) NOT NULL,
        locked_at TIMESTAMP(3) NOT NULL,
        locked_by VARCHAR(255) NOT NULL
      );
    """).fetch().rowsUpdated().then().block();
    }

    // Business Logic (Reactive)
    @Bean
    ReactiveMessageHandler superduperWorkerReactive() {
        return (MessageRow r) -> {
            System.out.println("[Reactive Worker] key=" + r.key() + " content=" + r.content());
            return Mono.just(ProcessingResult.SUCCESS);
        };
    }
}
