package net.rsworld.superduper.example.reactive;

import java.util.LinkedHashMap;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.worker.reactive.ReactiveRedriveService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/ops")
public class ExampleReactiveOperatorEndpoints {
    private final ReactiveRedriveService redriveService;
    private final ReactiveWorkerMessageRepository repository;

    public ExampleReactiveOperatorEndpoints(
            ReactiveRedriveService redriveService, ReactiveWorkerMessageRepository repository) {
        this.redriveService = redriveService;
        this.repository = repository;
    }

    @GetMapping("/queue")
    public Mono<Map<String, Long>> queueHealth() {
        return repository.countByStatus().map(ExampleReactiveOperatorEndpoints::normalizedCounts);
    }

    @GetMapping("/messages")
    public Flux<ClaimedMessage> inspect(@RequestParam String status, @RequestParam(defaultValue = "20") int limit) {
        return redriveService.inspect(status, limit);
    }

    @PostMapping("/redrive/{id}")
    public Mono<Map<String, Object>> redriveOne(@PathVariable long id) {
        return redriveService.redriveOne(id).map(redriven -> Map.of("id", id, "redriven", redriven));
    }

    @PostMapping("/redrive")
    public Mono<Map<String, Object>> redriveBatch(
            @RequestParam String status, @RequestParam(defaultValue = "100") int limit) {
        return redriveService
                .redriveBatch(status, limit)
                .map(redriven -> Map.of("status", status, "limit", limit, "redriven", redriven));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<Map<String, String>> handleInvalidRequest(IllegalArgumentException error) {
        return Mono.just(Map.of("error", error.getMessage()));
    }

    private static Map<String, Long> normalizedCounts(Map<String, Long> counts) {
        Map<String, Long> normalized = new LinkedHashMap<>();
        normalized.put("READY", counts.getOrDefault("READY", 0L));
        normalized.put("FAILED", counts.getOrDefault("FAILED", 0L));
        normalized.put("STOPPED", counts.getOrDefault("STOPPED", 0L));
        normalized.put("PROCESSED", counts.getOrDefault("PROCESSED", 0L));
        return normalized;
    }
}
