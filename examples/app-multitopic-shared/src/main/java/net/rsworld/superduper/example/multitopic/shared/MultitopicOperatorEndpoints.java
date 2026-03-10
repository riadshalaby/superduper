package net.rsworld.superduper.example.multitopic.shared;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.rsworld.superduper.repository.api.ClaimedMessage;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import net.rsworld.superduper.repository.api.WorkerMessageRepository;
import net.rsworld.superduper.starter.TopicRegistry;
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
public class MultitopicOperatorEndpoints {
    private final RedriveService redriveService;
    private final WorkerMessageRepository repository;
    private final Map<String, WorkerMessageRepository> repositoriesByTopic;
    private final List<String> configuredTopics;

    public MultitopicOperatorEndpoints(
            RedriveService redriveService,
            WorkerMessageRepository repository,
            TopicRegistry topicRegistry,
            TopicRepositoryFactory repositoryFactory) {
        this.redriveService = redriveService;
        this.repository = repository;
        this.repositoriesByTopic = buildRepositories(topicRegistry, repository, repositoryFactory);
        this.configuredTopics = topicRegistry.kafkaTopics();
    }

    @GetMapping("/queue")
    public Map<String, Long> queueHealth(@RequestParam(required = false) String topic) {
        if (hasText(topic)) {
            return normalizedCounts(repositoryFor(topic).countByStatus(topic));
        }
        Map<String, Long> aggregated = zeroCounts();
        for (String configuredTopic : configuredTopics) {
            addCounts(aggregated, repositoryFor(configuredTopic).countByStatus(configuredTopic));
        }
        return aggregated;
    }

    @GetMapping("/messages")
    public List<ClaimedMessage> inspect(
            @RequestParam String status,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(required = false) String topic) {
        if (hasText(topic)) {
            return redriveService.inspect(status, limit, topic);
        }
        return configuredTopics.stream()
                .flatMap(configuredTopic -> redriveService.inspect(status, limit, configuredTopic).stream())
                .sorted((left, right) -> {
                    int byTopic = left.topic().compareTo(right.topic());
                    if (byTopic != 0) {
                        return byTopic;
                    }
                    return left.id().compareTo(right.id());
                })
                .limit(limit)
                .toList();
    }

    @PostMapping("/redrive/{id}")
    public Map<String, Object> redriveOne(@PathVariable long id) {
        return Map.of("id", id, "redriven", redriveService.redriveOne(id));
    }

    @PostMapping("/redrive")
    public Map<String, Object> redriveBatch(
            @RequestParam String status,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String topic) {
        if (hasText(topic)) {
            return Map.of(
                    "status",
                    status,
                    "topic",
                    topic,
                    "limit",
                    limit,
                    "redriven",
                    redriveService.redriveBatch(status, limit, topic));
        }

        int remaining = limit;
        int redriven = 0;
        for (String configuredTopic : configuredTopics) {
            if (remaining <= 0) {
                break;
            }
            int topicRedriven = redriveService.redriveBatch(status, remaining, configuredTopic);
            redriven += topicRedriven;
            remaining -= topicRedriven;
        }
        return Map.of("status", status, "limit", limit, "redriven", redriven);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleInvalidRequest(IllegalArgumentException error) {
        return Map.of("error", error.getMessage());
    }

    private WorkerMessageRepository repositoryFor(String topic) {
        return repositoriesByTopic.getOrDefault(topic, repository);
    }

    private static Map<String, WorkerMessageRepository> buildRepositories(
            TopicRegistry topicRegistry,
            WorkerMessageRepository sharedRepository,
            TopicRepositoryFactory repositoryFactory) {
        Map<String, WorkerMessageRepository> repositories = new LinkedHashMap<>();
        for (var topic : topicRegistry.topics()) {
            if (topic.table().isBlank()) {
                repositories.put(topic.kafkaTopic(), sharedRepository);
                continue;
            }
            repositories.put(topic.kafkaTopic(), repositoryFactory.createWorkerRepository(topic.table()));
        }
        return repositories;
    }

    private static Map<String, Long> zeroCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("READY", 0L);
        counts.put("FAILED", 0L);
        counts.put("STOPPED", 0L);
        counts.put("PROCESSED", 0L);
        return counts;
    }

    private static void addCounts(Map<String, Long> aggregate, Map<String, Long> counts) {
        aggregate.compute("READY", (key, value) -> value + counts.getOrDefault("READY", 0L));
        aggregate.compute("FAILED", (key, value) -> value + counts.getOrDefault("FAILED", 0L));
        aggregate.compute("STOPPED", (key, value) -> value + counts.getOrDefault("STOPPED", 0L));
        aggregate.compute("PROCESSED", (key, value) -> value + counts.getOrDefault("PROCESSED", 0L));
    }

    private static Map<String, Long> normalizedCounts(Map<String, Long> counts) {
        Map<String, Long> normalized = zeroCounts();
        addCounts(normalized, counts);
        return normalized;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
