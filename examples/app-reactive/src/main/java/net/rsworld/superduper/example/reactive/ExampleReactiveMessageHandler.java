package net.rsworld.superduper.example.reactive;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.rsworld.superduper.worker.reactive.MessageRow;
import net.rsworld.superduper.worker.reactive.ProcessingResult;
import net.rsworld.superduper.worker.reactive.ReactiveMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
class ExampleReactiveMessageHandler implements ReactiveMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ExampleReactiveMessageHandler.class);

    private final SeedProgress progress;
    private final Map<Long, Integer> attempts = new ConcurrentHashMap<>();

    ExampleReactiveMessageHandler(SeedProgress progress) {
        this.progress = progress;
    }

    @Override
    public Mono<ProcessingResult> handle(MessageRow row) {
        return Mono.fromSupplier(() -> {
            int run = extractRun(row.content());
            progress.markHandled(row.id(), run);
            int attempt = attempts.merge(row.id(), 1, Integer::sum);
            String content = row.content() == null ? "" : row.content();

            if (content.contains("always-fail")) {
                log.warn(
                        "[Reactive Worker] id={} key={} attempt={} -> FAILURE (always-fail)",
                        row.id(),
                        row.key(),
                        attempt);
                return ProcessingResult.FAILURE;
            }

            if (content.contains("retry-once") && attempt == 1) {
                log.info(
                        "[Reactive Worker] id={} key={} attempt={} -> FAILURE (retry-once first attempt)",
                        row.id(),
                        row.key(),
                        attempt);
                return ProcessingResult.FAILURE;
            }

            log.info(
                    "[Reactive Worker] id={} key={} attempt={} -> SUCCESS content={}",
                    row.id(),
                    row.key(),
                    attempt,
                    content);
            return ProcessingResult.SUCCESS;
        });
    }

    private int extractRun(String content) {
        if (content == null) {
            return -1;
        }
        int marker = content.indexOf("run=");
        if (marker < 0) {
            return -1;
        }
        int start = marker + 4;
        int end = content.indexOf(' ', start);
        String value = end < 0 ? content.substring(start) : content.substring(start, end);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}
