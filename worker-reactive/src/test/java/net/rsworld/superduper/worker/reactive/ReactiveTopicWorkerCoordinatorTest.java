package net.rsworld.superduper.worker.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveWorkerMessageRepository;
import net.rsworld.superduper.repository.api.TopicConfigView;
import net.rsworld.superduper.repository.api.TopicRegistryView;
import net.rsworld.superduper.repository.api.TopicRepositoryFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.TaskScheduler;
import reactor.core.publisher.Mono;

class ReactiveTopicWorkerCoordinatorTest {

    private static TopicConfigView topicConfig(
            String name, String kafkaTopic, String handlerBeanName, String claimLockName) {
        return topicConfig(name, kafkaTopic, handlerBeanName, claimLockName, "", null);
    }

    private static TopicConfigView topicConfig(
            String name, String kafkaTopic, String handlerBeanName, String claimLockName, String table) {
        return topicConfig(name, kafkaTopic, handlerBeanName, claimLockName, table, null);
    }

    private static TopicConfigView topicConfig(
            String name,
            String kafkaTopic,
            String handlerBeanName,
            String claimLockName,
            String table,
            String topicColumnValue) {
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
                return table;
            }

            @Override
            public String claimLockName() {
                return claimLockName;
            }

            @Override
            public String topicColumnValue() {
                return topicColumnValue == null ? kafkaTopic : topicColumnValue;
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

    @Test
    void topicWorkerInstance_usesTopicColumnValueWhenConfigured() {
        TopicConfigView config = topicConfig(
                "orders", "orders-kafka-topic", "handlerA", "superduper-claim-orders", "", "orders-column-value");
        ReactiveWorkerMessageRepository messageRepository = mock(ReactiveWorkerMessageRepository.class);
        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        ReactiveMessageHandler handler = mock(ReactiveMessageHandler.class);

        when(messageRepository.claimBatch(anyString(), anyInt(), anyInt(), anyString()))
                .thenReturn(Mono.just(0L));
        org.mockito.Mockito.doAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());

        ReactiveTopicWorkerInstance instance = new ReactiveTopicWorkerInstance(
                config,
                messageRepository,
                lockExec,
                handler,
                NoopSuperduperObserver.INSTANCE,
                "test-worker",
                15000,
                2000);

        instance.claimAndProcess();

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(messageRepository).claimBatch(anyString(), anyInt(), anyInt(), topicCaptor.capture());
        assertThat(topicCaptor.getValue()).isEqualTo("orders-column-value");
    }

    @Test
    void coordinator_usesDedicatedRepositoryForConfiguredTable() {
        TopicConfigView dedicatedTopic =
                topicConfig("orders", "orders-topic", "handlerA", "superduper-claim-orders", "orders_messages");
        TopicRegistryView registry = registryOf(dedicatedTopic);

        ReactiveWorkerMessageRepository sharedRepository = mock(ReactiveWorkerMessageRepository.class);
        ReactiveWorkerMessageRepository dedicatedRepository = mock(ReactiveWorkerMessageRepository.class);
        TopicRepositoryFactory repositoryFactory = mock(TopicRepositoryFactory.class);
        when(repositoryFactory.createReactiveWorkerRepository("orders_messages"))
                .thenReturn(dedicatedRepository);
        when(dedicatedRepository.claimBatch(anyString(), anyInt(), anyInt(), anyString()))
                .thenReturn(Mono.just(0L));

        LockingTaskExecutor lockExec = mock(LockingTaskExecutor.class);
        org.mockito.Mockito.doAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run();
                    return null;
                })
                .when(lockExec)
                .executeWithLock(any(Runnable.class), any());

        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(future)
                .when(taskScheduler)
                .scheduleWithFixedDelay(runnableCaptor.capture(), any(Instant.class), any(Duration.class));

        ReactiveTopicWorkerCoordinator coordinator = new ReactiveTopicWorkerCoordinator(
                registry,
                sharedRepository,
                lockExec,
                Map.of("handlerA", mock(ReactiveMessageHandler.class)),
                NoopSuperduperObserver.INSTANCE,
                repositoryFactory,
                taskScheduler,
                1000,
                100,
                15000,
                2000);

        coordinator.start();
        runnableCaptor.getValue().run();

        verify(repositoryFactory).createReactiveWorkerRepository("orders_messages");
        verify(dedicatedRepository).claimBatch(anyString(), anyInt(), anyInt(), eq("orders-topic"));
        verify(sharedRepository, never()).claimBatch(anyString(), anyInt(), anyInt(), anyString());
    }
}
