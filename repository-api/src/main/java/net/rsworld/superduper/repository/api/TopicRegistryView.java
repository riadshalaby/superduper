package net.rsworld.superduper.repository.api;

import java.util.List;

public interface TopicRegistryView {
    List<? extends TopicConfigView> topics();

    List<String> kafkaTopics();

    TopicConfigView getByKafkaTopic(String kafkaTopic);
}
