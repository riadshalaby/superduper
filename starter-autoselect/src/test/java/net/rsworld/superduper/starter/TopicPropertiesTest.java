package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicPropertiesTest {

    @Test
    void normalizesNullMapsAndTables() {
        TopicProperties properties = new TopicProperties();
        TopicProperties.TopicConfig config = new TopicProperties.TopicConfig();

        properties.setTopics(null);
        config.setTable(null);

        assertThat(properties.getTopics()).isEmpty();
        assertThat(config.getTable()).isEmpty();
        assertThat(config.getBatchSize()).isEqualTo(-1);
        assertThat(config.getMaxRetries()).isEqualTo(-1);
    }

    @Test
    void storesConfiguredTopicValues() {
        TopicProperties properties = new TopicProperties();
        TopicProperties.TopicConfig config = new TopicProperties.TopicConfig();
        config.setKafkaTopic("orders.events");
        config.setHandler("ordersHandler");
        config.setBatchSize(25);
        config.setMaxRetries(8);
        config.setTable("orders_messages");
        properties.setTopics(Map.of("orders", config));

        assertThat(properties.getTopics()).containsOnlyKeys("orders");
        TopicProperties.TopicConfig stored = properties.getTopics().get("orders");
        assertThat(stored.getKafkaTopic()).isEqualTo("orders.events");
        assertThat(stored.getHandler()).isEqualTo("ordersHandler");
        assertThat(stored.getBatchSize()).isEqualTo(25);
        assertThat(stored.getMaxRetries()).isEqualTo(8);
        assertThat(stored.getTable()).isEqualTo("orders_messages");
    }
}
