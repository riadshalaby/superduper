package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicRegistryTest {

    @Test
    void exposesConfiguredTopicsAndKafkaTopicLookup() {
        TopicRegistry.ResolvedTopicConfig shared = new TopicRegistry.ResolvedTopicConfig(
                "orders", "orders.events", "ordersHandler", 10, 5, "", "orders-claim");
        TopicRegistry.ResolvedTopicConfig dedicated = new TopicRegistry.ResolvedTopicConfig(
                "invoices", "invoices.events", "invoicesHandler", 20, 7, "invoice_messages", "invoices-claim");

        TopicRegistry registry = new TopicRegistry(List.of(shared, dedicated));

        assertThat(registry.topics()).containsExactly(shared, dedicated);
        assertThat(registry.kafkaTopics()).containsExactly("orders.events", "invoices.events");
        assertThat(registry.getByKafkaTopic("invoices.events")).isSameAs(dedicated);
        assertThatThrownBy(() -> registry.getByKafkaTopic("missing.events"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No topic configuration found");
    }

    @Test
    void rejectsDuplicateKafkaTopics() {
        TopicRegistry.ResolvedTopicConfig first =
                new TopicRegistry.ResolvedTopicConfig("orders", "shared.events", "a", 10, 5, "", "lock-a");
        TopicRegistry.ResolvedTopicConfig second =
                new TopicRegistry.ResolvedTopicConfig("invoices", "shared.events", "b", 10, 5, "", "lock-b");

        assertThatThrownBy(() -> new TopicRegistry(List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate Kafka topic configured");
    }
}
