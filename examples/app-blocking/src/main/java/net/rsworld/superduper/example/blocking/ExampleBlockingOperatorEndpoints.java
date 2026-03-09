package net.rsworld.superduper.example.blocking;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.worker.blocking.RedriveService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ops")
public class ExampleBlockingOperatorEndpoints {
    private final RedriveService redriveService;
    private final WorkerMessageRepository repository;

    public ExampleBlockingOperatorEndpoints(RedriveService redriveService, WorkerMessageRepository repository) {
        this.redriveService = redriveService;
        this.repository = repository;
    }

    @GetMapping("/queue")
    public Map<String, Long> queueHealth() {
        return normalizedCounts(repository.countByStatus());
    }

    @GetMapping("/messages")
    public List<ClaimedMessage> inspect(@RequestParam String status, @RequestParam(defaultValue = "20") int limit) {
        return redriveService.inspect(status, limit);
    }

    @PostMapping("/redrive/{id}")
    public Map<String, Object> redriveOne(@PathVariable long id) {
        return Map.of("id", id, "redriven", redriveService.redriveOne(id));
    }

    @PostMapping("/redrive")
    public Map<String, Object> redriveBatch(
            @RequestParam String status, @RequestParam(defaultValue = "100") int limit) {
        return Map.of("status", status, "limit", limit, "redriven", redriveService.redriveBatch(status, limit));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleInvalidRequest(IllegalArgumentException error) {
        return Map.of("error", error.getMessage());
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
