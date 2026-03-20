package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxPropertiesTest {

    @Test
    void normalizesNullMapsAndTables() {
        OutboxProperties properties = new OutboxProperties();
        OutboxProperties.OutboxConfig config = new OutboxProperties.OutboxConfig();

        properties.setOutbox(null);
        config.setTable(null);

        assertThat(properties.getOutbox()).isEmpty();
        assertThat(config.getTable()).isEmpty();
        assertThat(config.getBatchSize()).isZero();
        assertThat(config.getMaxRetries()).isZero();
    }

    @Test
    void storesConfiguredOutboxValues() {
        OutboxProperties properties = new OutboxProperties();
        OutboxProperties.OutboxConfig config = new OutboxProperties.OutboxConfig();
        config.setHandler("ordersOutboxHandler");
        config.setBatchSize(25);
        config.setMaxRetries(8);
        config.setTable("orders_outbox_messages");
        properties.setOutbox(Map.of("orders-outbox", config));

        assertThat(properties.getOutbox()).containsOnlyKeys("orders-outbox");
        OutboxProperties.OutboxConfig stored = properties.getOutbox().get("orders-outbox");
        assertThat(stored.getHandler()).isEqualTo("ordersOutboxHandler");
        assertThat(stored.getBatchSize()).isEqualTo(25);
        assertThat(stored.getMaxRetries()).isEqualTo(8);
        assertThat(stored.getTable()).isEqualTo("orders_outbox_messages");
    }
}
