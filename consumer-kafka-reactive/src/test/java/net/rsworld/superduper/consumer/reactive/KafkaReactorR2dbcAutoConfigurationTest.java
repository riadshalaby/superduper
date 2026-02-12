package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

class KafkaReactorR2dbcAutoConfigurationTest {
    @Test
    void springKafkaBeans_build() {
        KafkaReactorR2dbcAutoConfiguration cfg = new KafkaReactorR2dbcAutoConfiguration();
        ConsumerFactory<String, String> cf = cfg.reactiveConsumerFactory("localhost:9092", "g");
        assertThat(cf).isNotNull();
        ConcurrentKafkaListenerContainerFactory<String, String> factory = cfg.reactiveKafkaListenerContainerFactory(cf);
        assertThat(factory).isNotNull();
        assertThat(factory.getContainerProperties().getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    }
}
