package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

class ReactiveTopicWorkerCoordinatorTest {

    private static TopicConfigView topicConfig(
            String name, String kafkaTopic, String handlerBeanName, String claimLockName) {
        return new TopicConfigView() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String kafkaTopic() {
                return kafkaTopic;
            }

            @Override
            public String handlerBeanName() {
                return handlerBeanName;
            }

            @Override
            public int batchSize() {
                return 10;
            }

            @Override
            public int maxRetries() {
                return 3;
            }

            @Override
            public String table() {
                return "";
            }

            @Override
            public String claimLockName() {
                return claimLockName;
            }
        };
    }

    private static TopicRegistryView registryOf(TopicConfigView... configs) {
        List<TopicConfigView> list = List.of(configs);
        return new TopicRegistryView() {
            @Override
            public List<? extends TopicConfigView> topics() {
                return list;
            }

            @Override
            public List<String> kafkaTopics() {
                return list.stream().map(TopicConfigView::kafkaTopic).toList();
            }

            @Override
            public TopicConfigView getByKafkaTopic(String kafkaTopic) {
                return list.stream()
                        .filter(t -> t.kafkaTopic().equals(kafkaTopic))
                        .findFirst()
                        .orElseThrow();
            }
        };
    }

    @Test
    void coordinator_throwsIfHandlerMissing() {
        TopicRegistryView registry =
                registryOf(topicConfig("orders", "orders-topic", "myHandler", "superduper-claim-orders"));

        assertThatThrownBy(() -> new ReactiveTopicWorkerCoordinator(
                        registry,
                        mock(ReactiveWorkerMessageRepository.class),
                        mock(LockingTaskExecutor.class),
                        Map.of(),
                        NoopSuperduperObserver.INSTANCE,
                        mock(TopicRepositoryFactory.class),
                        mock(TaskScheduler.class),
                        1000,
                        100,
                        15000,
                        2000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("myHandler");
    }

    @Test
    void coordinator_schedulesTwoTasksForTwoTopics() {
        TopicConfigView configA = topicConfig("topic-a", "topic-a", "handlerA", "superduper-claim-topic-a");
        TopicConfigView configB = topicConfig("topic-b", "topic-b", "handlerB", "superduper-claim-topic-b");
        TopicRegistryView registry = registryOf(configA, configB);

        ReactiveMessageHandler handlerA = mock(ReactiveMessageHandler.class);
        ReactiveMessageHandler handlerB = mock(ReactiveMessageHandler.class);
        TaskScheduler taskScheduler = mock(TaskScheduler.class);

        ReactiveTopicWorkerCoordinator coordinator = new ReactiveTopicWorkerCoordinator(
                registry,
                mock(ReactiveWorkerMessageRepository.class),
                mock(LockingTaskExecutor.class),
                Map.of("handlerA", handlerA, "handlerB", handlerB),
                NoopSuperduperObserver.INSTANCE,
                mock(TopicRepositoryFactory.class),
                taskScheduler,
                1000,
                100,
                15000,
                2000);

        coordinator.start();

        verify(taskScheduler, times(2))
                .scheduleWithFixedDelay(any(Runnable.class), any(Instant.class), any(Duration.class));
    }
}
