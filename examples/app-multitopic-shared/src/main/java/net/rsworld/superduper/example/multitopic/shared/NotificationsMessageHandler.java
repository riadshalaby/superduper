package net.rsworld.superduper.example.multitopic.shared;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.rsworld.superduper.worker.blocking.MessageHandler;
import net.rsworld.superduper.worker.blocking.MessageRow;
import net.rsworld.superduper.worker.blocking.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("notificationsMessageHandler")
class NotificationsMessageHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(NotificationsMessageHandler.class);

    private final SeedProgress progress;
    private final Map<Long, Integer> attempts = new ConcurrentHashMap<>();

    NotificationsMessageHandler(SeedProgress progress) {
        this.progress = progress;
    }

    @Override
    public ProcessingResult handle(MessageRow row) {
        String runToken = extractRunToken(row.content());
        progress.markHandled(row.messageId(), runToken);
        int attempt = attempts.merge(row.id(), 1, Integer::sum);
        String content = row.content() == null ? "" : row.content();

        if (content.contains("always-fail")) {
            log.warn(
                    "[Notifications] id={} key={} attempt={} -> FAILURE (always-fail)",
                    row.id(),
                    row.messageKey(),
                    attempt);
            return ProcessingResult.FAILURE;
        }

        if (content.contains("retry-once") && attempt == 1) {
            log.info(
                    "[Notifications] id={} key={} attempt={} -> FAILURE (retry-once first attempt)",
                    row.id(),
                    row.messageKey(),
                    attempt);
            return ProcessingResult.FAILURE;
        }

        log.info(
                "[Notifications] id={} key={} attempt={} -> SUCCESS content={}",
                row.id(),
                row.messageKey(),
                attempt,
                content);
        return ProcessingResult.SUCCESS;
    }

    private static String extractRunToken(String content) {
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
