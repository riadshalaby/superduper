package net.rsworld.superduper.repository.api;

/** Creates topic-scoped repository adapters backed by a specific storage table. */
public interface TopicRepositoryFactory {
    /**
     * Creates a blocking ingest repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the blocking ingest repository
     */
    MessageIngestRepository createIngestRepository(String tableName);

    /**
     * Creates a blocking worker message repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the blocking worker message repository
     */
    WorkerMessageRepository createWorkerRepository(String tableName);

    /**
     * Creates a blocking worker maintenance repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the blocking worker maintenance repository
     */
    WorkerMaintenanceRepository createMaintenanceRepository(String tableName);

    /**
     * Creates a reactive ingest repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the reactive ingest repository
     */
    ReactiveMessageIngestRepository createReactiveIngestRepository(String tableName);

    /**
     * Creates a reactive worker message repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the reactive worker message repository
     */
    ReactiveWorkerMessageRepository createReactiveWorkerRepository(String tableName);

    /**
     * Creates a reactive worker maintenance repository for the given table.
     *
     * @param tableName the storage table name backing the repository
     * @return the reactive worker maintenance repository
     */
    ReactiveWorkerMaintenanceRepository createReactiveMaintenanceRepository(String tableName);
}
