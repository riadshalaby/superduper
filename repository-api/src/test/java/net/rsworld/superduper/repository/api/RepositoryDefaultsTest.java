package net.rsworld.superduper.repository.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class RepositoryDefaultsTest {

    @Test
    void blockingMessageIngestDefaultsUseDefaultTopicAndIterateBatch() {
        RecordingMessageIngestRepository repository = new RecordingMessageIngestRepository();
        Instant occurredAt = Instant.parse("2026-03-16T00:00:00Z");

        repository.upsertReadyMessage("id-1", "key-1", "body-1", occurredAt, "corr-1", "type-1");
        repository.batchUpsertReadyMessages(List.of(
                new MessageIngestData("topic-a", "id-2", "key-2", "body-2", occurredAt, "corr-2", "type-2"),
                new MessageIngestData("topic-b", "id-3", "key-3", "body-3", occurredAt, "corr-3", "type-3")));

        assertThat(repository.calls)
                .containsExactly(
                        new IngestCall("default", "id-1", "key-1", "body-1", occurredAt, "corr-1", "type-1"),
                        new IngestCall("topic-a", "id-2", "key-2", "body-2", occurredAt, "corr-2", "type-2"),
                        new IngestCall("topic-b", "id-3", "key-3", "body-3", occurredAt, "corr-3", "type-3"));
        assertThatThrownBy(() -> repository.batchUpsertReadyMessages(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void reactiveMessageIngestDefaultsUseDefaultTopicAndIterateBatch() {
        RecordingReactiveMessageIngestRepository repository = new RecordingReactiveMessageIngestRepository();
        Instant occurredAt = Instant.parse("2026-03-16T00:00:00Z");

        repository
                .upsertReadyMessage("id-1", "key-1", "body-1", occurredAt, "corr-1", "type-1")
                .block();
        repository
                .batchUpsertReadyMessages(List.of(
                        new MessageIngestData("topic-a", "id-2", "key-2", "body-2", occurredAt, "corr-2", "type-2"),
                        new MessageIngestData("topic-b", "id-3", "key-3", "body-3", occurredAt, "corr-3", "type-3")))
                .block();

        assertThat(repository.calls)
                .containsExactly(
                        new IngestCall("default", "id-1", "key-1", "body-1", occurredAt, "corr-1", "type-1"),
                        new IngestCall("topic-a", "id-2", "key-2", "body-2", occurredAt, "corr-2", "type-2"),
                        new IngestCall("topic-b", "id-3", "key-3", "body-3", occurredAt, "corr-3", "type-3"));
        assertThatThrownBy(() -> repository.batchUpsertReadyMessages(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void blockingMaintenanceDefaultsDelegateToDefaultTopic() {
        RecordingWorkerMaintenanceRepository repository = new RecordingWorkerMaintenanceRepository();

        assertThat(repository.reclaimStaleProcessing(12)).isEqualTo(12);
        assertThat(repository.reclaimMissingHeartbeats(34)).isEqualTo(34);
        assertThat(repository.deleteProcessedOlderThan(5)).isEqualTo(5);
        assertThat(repository.deleteStoppedOlderThan(6)).isEqualTo(6);

        assertThat(repository.calls)
                .containsExactly(
                        "reclaimStaleProcessing:default:12",
                        "reclaimMissingHeartbeats:default:34",
                        "deleteProcessedOlderThan:default:5",
                        "deleteStoppedOlderThan:default:6");
    }

    @Test
    void reactiveMaintenanceDefaultsDelegateToDefaultTopic() {
        RecordingReactiveWorkerMaintenanceRepository repository = new RecordingReactiveWorkerMaintenanceRepository();

        assertThat(repository.reclaimStaleProcessing(12).block()).isEqualTo(12);
        assertThat(repository.reclaimMissingHeartbeats(34).block()).isEqualTo(34);
        assertThat(repository.deleteProcessedOlderThan(5).block()).isEqualTo(5);
        assertThat(repository.deleteStoppedOlderThan(6).block()).isEqualTo(6);

        assertThat(repository.calls)
                .containsExactly(
                        "reclaimStaleProcessing:default:12",
                        "reclaimMissingHeartbeats:default:34",
                        "deleteProcessedOlderThan:default:5",
                        "deleteStoppedOlderThan:default:6");
    }

    @Test
    void blockingWorkerDefaultsDelegateToDefaultTopic() {
        RecordingWorkerMessageRepository repository = new RecordingWorkerMessageRepository();

        assertThat(repository.claimBatch("worker-a", 10, 3)).isEqualTo(13L);
        assertThat(repository.fetchClaimedForWorker("worker-a")).containsExactly(repository.claimedMessage);
        assertThat(repository.findByStatus("FAILED", 4)).containsExactly(repository.claimedMessage);
        assertThat(repository.redriveByStatus("FAILED", 5)).isEqualTo(5);
        assertThat(repository.countByStatus()).containsEntry("READY", 1L);

        assertThat(repository.calls)
                .containsExactly(
                        "claimBatch:default:worker-a:10:3",
                        "fetchClaimedForWorker:default:worker-a",
                        "findByStatus:default:FAILED:4",
                        "redriveByStatus:default:FAILED:5",
                        "countByStatus:default");
    }

    @Test
    void reactiveWorkerDefaultsDelegateToDefaultTopic() {
        RecordingReactiveWorkerMessageRepository repository = new RecordingReactiveWorkerMessageRepository();

        assertThat(repository.claimBatch("worker-a", 10, 3).block()).isEqualTo(13L);
        assertThat(repository.fetchClaimedForWorker("worker-a").collectList().block())
                .containsExactly(repository.claimedMessage);
        assertThat(repository.findByStatus("FAILED", 4).collectList().block())
                .containsExactly(repository.claimedMessage);
        assertThat(repository.redriveByStatus("FAILED", 5).block()).isEqualTo(5);
        assertThat(repository.countByStatus().block()).containsEntry("READY", 1L);

        assertThat(repository.calls)
                .containsExactly(
                        "claimBatch:default:worker-a:10:3",
                        "fetchClaimedForWorker:default:worker-a",
                        "findByStatus:default:FAILED:4",
                        "redriveByStatus:default:FAILED:5",
                        "countByStatus:default");
    }

    @Test
    void claimedMessageConvenienceConstructorUsesDefaultTopic() {
        ClaimedMessage message =
                new ClaimedMessage(7L, "message-7", "key-7", "body-7", 2, "worker-a", "corr-7", "type-7");
        MessageIngestData ingestData = new MessageIngestData(
                "topic-a", "message-8", "key-8", "body-8", Instant.parse("2026-03-16T00:00:00Z"), "corr-8", "type-8");

        assertThat(message.topic()).isEqualTo("default");
        assertThat(message.messageId()).isEqualTo("message-7");
        assertThat(ingestData.topic()).isEqualTo("topic-a");
        assertThat(ingestData.messageType()).isEqualTo("type-8");
    }

    private record IngestCall(
            String topic,
            String messageId,
            String messageKey,
            String content,
            Instant occurredAt,
            String correlationId,
            String messageType) {}

    private static final class RecordingMessageIngestRepository implements MessageIngestRepository {
        private final java.util.ArrayList<IngestCall> calls = new java.util.ArrayList<>();

        @Override
        public void upsertReadyMessage(
                String topic,
                String messageId,
                String messageKey,
                String content,
                Instant occurredAt,
                String correlationId,
                String messageType) {
            calls.add(new IngestCall(topic, messageId, messageKey, content, occurredAt, correlationId, messageType));
        }
    }

    private static final class RecordingReactiveMessageIngestRepository implements ReactiveMessageIngestRepository {
        private final java.util.ArrayList<IngestCall> calls = new java.util.ArrayList<>();

        @Override
        public Mono<Void> upsertReadyMessage(
                String topic,
                String messageId,
                String messageKey,
                String content,
                Instant occurredAt,
                String correlationId,
                String messageType) {
            calls.add(new IngestCall(topic, messageId, messageKey, content, occurredAt, correlationId, messageType));
            return Mono.empty();
        }
    }

    private static final class RecordingWorkerMaintenanceRepository implements WorkerMaintenanceRepository {
        private final java.util.ArrayList<String> calls = new java.util.ArrayList<>();

        @Override
        public void heartbeat(String workerId) {}

        @Override
        public int reclaimStaleProcessing(int orphanTimeoutSec, String topic) {
            calls.add("reclaimStaleProcessing:" + topic + ":" + orphanTimeoutSec);
            return orphanTimeoutSec;
        }

        @Override
        public int reclaimMissingHeartbeats(int heartbeatWindowSec, String topic) {
            calls.add("reclaimMissingHeartbeats:" + topic + ":" + heartbeatWindowSec);
            return heartbeatWindowSec;
        }

        @Override
        public int deleteProcessedOlderThan(int retentionDays, String topic) {
            calls.add("deleteProcessedOlderThan:" + topic + ":" + retentionDays);
            return retentionDays;
        }

        @Override
        public int deleteStoppedOlderThan(int retentionDays, String topic) {
            calls.add("deleteStoppedOlderThan:" + topic + ":" + retentionDays);
            return retentionDays;
        }

        @Override
        public int deleteStaleHeartbeats(int retentionDays) {
            return retentionDays;
        }
    }

    private static final class RecordingReactiveWorkerMaintenanceRepository
            implements ReactiveWorkerMaintenanceRepository {
        private final java.util.ArrayList<String> calls = new java.util.ArrayList<>();

        @Override
        public Mono<Void> heartbeat(String workerId) {
            return Mono.empty();
        }

        @Override
        public Mono<Integer> reclaimStaleProcessing(int orphanTimeoutSec, String topic) {
            calls.add("reclaimStaleProcessing:" + topic + ":" + orphanTimeoutSec);
            return Mono.just(orphanTimeoutSec);
        }

        @Override
        public Mono<Integer> reclaimMissingHeartbeats(int heartbeatWindowSec, String topic) {
            calls.add("reclaimMissingHeartbeats:" + topic + ":" + heartbeatWindowSec);
            return Mono.just(heartbeatWindowSec);
        }

        @Override
        public Mono<Integer> deleteProcessedOlderThan(int retentionDays, String topic) {
            calls.add("deleteProcessedOlderThan:" + topic + ":" + retentionDays);
            return Mono.just(retentionDays);
        }

        @Override
        public Mono<Integer> deleteStoppedOlderThan(int retentionDays, String topic) {
            calls.add("deleteStoppedOlderThan:" + topic + ":" + retentionDays);
            return Mono.just(retentionDays);
        }

        @Override
        public Mono<Integer> deleteStaleHeartbeats(int retentionDays) {
            return Mono.just(retentionDays);
        }
    }

    private static final class RecordingWorkerMessageRepository implements WorkerMessageRepository {
        private final java.util.ArrayList<String> calls = new java.util.ArrayList<>();
        private final ClaimedMessage claimedMessage =
                new ClaimedMessage(1L, "default", "message-1", "key-1", "body-1", 0, "worker-a", "corr-1", "type-1");

        @Override
        public long claimBatch(String workerId, int batchSize, int maxRetries, String topic) {
            calls.add("claimBatch:" + topic + ":" + workerId + ":" + batchSize + ":" + maxRetries);
            return batchSize + maxRetries;
        }

        @Override
        public List<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic) {
            calls.add("fetchClaimedForWorker:" + topic + ":" + workerId);
            return List.of(claimedMessage);
        }

        @Override
        public List<ClaimedMessage> findByStatus(String status, int limit, String topic) {
            calls.add("findByStatus:" + topic + ":" + status + ":" + limit);
            return List.of(claimedMessage);
        }

        @Override
        public int releaseMessages(List<Long> ids, String containerId) {
            return ids.size();
        }

        @Override
        public boolean markProcessed(long id, String containerId) {
            return true;
        }

        @Override
        public boolean markFailed(long id, int retryCount, String containerId) {
            return true;
        }

        @Override
        public boolean markStopped(long id, int retryCount, String containerId) {
            return true;
        }

        @Override
        public int redriveById(long id) {
            return 1;
        }

        @Override
        public int redriveByStatus(String status, int limit, String topic) {
            calls.add("redriveByStatus:" + topic + ":" + status + ":" + limit);
            return limit;
        }

        @Override
        public Map<String, Long> countByStatus(String topic) {
            calls.add("countByStatus:" + topic);
            return Map.of("READY", 1L);
        }
    }

    private static final class RecordingReactiveWorkerMessageRepository implements ReactiveWorkerMessageRepository {
        private final java.util.ArrayList<String> calls = new java.util.ArrayList<>();
        private final ClaimedMessage claimedMessage =
                new ClaimedMessage(1L, "default", "message-1", "key-1", "body-1", 0, "worker-a", "corr-1", "type-1");

        @Override
        public Mono<Long> claimBatch(String workerId, int batchSize, int maxRetries, String topic) {
            calls.add("claimBatch:" + topic + ":" + workerId + ":" + batchSize + ":" + maxRetries);
            return Mono.just((long) batchSize + maxRetries);
        }

        @Override
        public Flux<ClaimedMessage> fetchClaimedForWorker(String workerId, String topic) {
            calls.add("fetchClaimedForWorker:" + topic + ":" + workerId);
            return Flux.just(claimedMessage);
        }

        @Override
        public Flux<ClaimedMessage> findByStatus(String status, int limit, String topic) {
            calls.add("findByStatus:" + topic + ":" + status + ":" + limit);
            return Flux.just(claimedMessage);
        }

        @Override
        public Mono<Integer> releaseMessages(List<Long> ids, String containerId) {
            return Mono.just(ids.size());
        }

        @Override
        public Mono<Boolean> markProcessed(long id, String containerId) {
            return Mono.just(true);
        }

        @Override
        public Mono<Boolean> markFailed(long id, int retryCount, String containerId) {
            return Mono.just(true);
        }

        @Override
        public Mono<Boolean> markStopped(long id, int retryCount, String containerId) {
            return Mono.just(true);
        }

        @Override
        public Mono<Integer> redriveById(long id) {
            return Mono.just(1);
        }

        @Override
        public Mono<Integer> redriveByStatus(String status, int limit, String topic) {
            calls.add("redriveByStatus:" + topic + ":" + status + ":" + limit);
            return Mono.just(limit);
        }

        @Override
        public Mono<Map<String, Long>> countByStatus(String topic) {
            calls.add("countByStatus:" + topic);
            return Mono.just(Map.of("READY", 1L));
        }
    }
}
