package net.rsworld.superduper.example.jdbc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.rsworld.superduper.worker.jdbc.MessageHandler;
import net.rsworld.superduper.worker.jdbc.MessageRow;
import net.rsworld.superduper.worker.jdbc.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
class ExampleJdbcMessageHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ExampleJdbcMessageHandler.class);

    private final SeedProgress progress;
    private final Map<Long, Integer> attempts = new ConcurrentHashMap<>();

    ExampleJdbcMessageHandler(SeedProgress progress) {
        this.progress = progress;
    }

    @Override
    public ProcessingResult handle(MessageRow row) {
        String runToken = extractRunToken(row.content());
        progress.markHandled(row.id(), runToken);
        int attempt = attempts.merge(row.id(), 1, Integer::sum);
        String content = row.content() == null ? "" : row.content();

        if (content.contains("always-fail")) {
            log.warn("[JDBC Worker] id={} key={} attempt={} -> RETRY (always-fail)", row.id(), row.key(), attempt);
            return ProcessingResult.RETRY;
        }

        if (content.contains("retry-once") && attempt == 1) {
            log.info(
                    "[JDBC Worker] id={} key={} attempt={} -> RETRY (retry-once first attempt)",
                    row.id(),
                    row.key(),
                    attempt);
            return ProcessingResult.RETRY;
        }

        log.info("[JDBC Worker] id={} key={} attempt={} -> SUCCESS content={}", row.id(), row.key(), attempt, content);
        return ProcessingResult.SUCCESS;
    }

    private String extractRunToken(String content) {
        if (content == null) {
            return null;
        }
        int marker = content.indexOf("runToken=");
        if (marker < 0) {
            return null;
        }
        int start = marker + 9;
        int end = content.indexOf(' ', start);
        return end < 0 ? content.substring(start) : content.substring(start, end);
    }
}
