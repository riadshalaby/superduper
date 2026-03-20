package net.rsworld.superduper.example.multitopic.shared;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.outbox.blocking.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
class OutboxSeedService {
    private static final Logger log = LoggerFactory.getLogger(OutboxSeedService.class);

    private final OutboxService outboxService;

    OutboxSeedService(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @Transactional
    Map<String, Integer> seedNotifications(int count, int keyCount, String runToken) {
        long startedAt = System.nanoTime();
        for (int i = 1; i <= count; i++) {
            outboxService.send("notifications", notificationKeyFor(i, keyCount), contentFor(i, runToken));
        }
        long millis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        log.info(
                "[Seeder] Persisted {} outbox messages for notifications inside a single @Transactional method in {}ms.",
                count,
                millis);
        return new LinkedHashMap<>(Map.of("notifications", count));
    }

    private static String notificationKeyFor(int index, int keyCount) {
        return "notification-" + (index % keyCount);
    }

    private static String contentFor(int ordinal, String runToken) {
        if (ordinal % 40 == 0) {
            return "always-fail runToken=" + runToken + " #" + ordinal;
        }
        if (ordinal % 10 == 0) {
            return "retry-once runToken=" + runToken + " #" + ordinal;
        }
        return "ok runToken=" + runToken + " #" + ordinal;
    }
}
