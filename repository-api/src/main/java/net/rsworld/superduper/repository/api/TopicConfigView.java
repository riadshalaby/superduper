package net.rsworld.superduper.repository.api;

public interface TopicConfigView {
    String name();

    String kafkaTopic();

    String handlerBeanName();

    int batchSize();

    int maxRetries();

    String table();

    String claimLockName();
}
