package net.rsworld.superduper.repository.api;

public interface TopicRepositoryFactory {
    MessageIngestRepository createIngestRepository(String tableName);

    WorkerMessageRepository createWorkerRepository(String tableName);

    WorkerMaintenanceRepository createMaintenanceRepository(String tableName);

    ReactiveMessageIngestRepository createReactiveIngestRepository(String tableName);

    ReactiveWorkerMessageRepository createReactiveWorkerRepository(String tableName);

    ReactiveWorkerMaintenanceRepository createReactiveMaintenanceRepository(String tableName);
}
