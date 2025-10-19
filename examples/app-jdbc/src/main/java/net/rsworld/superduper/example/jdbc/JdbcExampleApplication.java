package net.rsworld.superduper.example.jdbc;

import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.MessageRow;
import net.rsworld.superduper.worker.jdbc.ProcessingResult;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class JdbcExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(JdbcExampleApplication.class, args);
    }

    // Business Logic (JDBC)
    @Bean
    MessageHandler superduperWorker() {
        return (MessageRow r) -> {
            System.out.println("[JDBC Worker] key=" + r.key() + " content=" + r.content());
            return ProcessingResult.SUCCESS;
        };
    }
}
