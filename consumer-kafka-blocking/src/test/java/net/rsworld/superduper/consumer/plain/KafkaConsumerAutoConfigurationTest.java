package net.rsworld.superduper.consumer.plain;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

class KafkaConsumerAutoConfigurationTest {
    @Test
    void springKafkaBeans_buildBatchListenerFactory() {
        KafkaConsumerAutoConfiguration cfg = new KafkaConsumerAutoConfiguration();
        ConsumerFactory<String, String> cf = cfg.consumerFactory("localhost:9092", "g", 42);
        assertThat(cf).isInstanceOf(DefaultKafkaConsumerFactory.class);
        assertThat(((DefaultKafkaConsumerFactory<String, String>) cf).getConfigurationProperties())
                .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 42);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = cfg.kafkaListenerContainerFactory(cf);
        assertThat(factory).isNotNull();
        assertThat(factory.isBatchListener()).isTrue();
        assertThat(factory.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL);
    }
}
