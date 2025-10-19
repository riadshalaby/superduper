package net.rsworld.superduper.consumer.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import reactor.kafka.receiver.ReceiverOptions;

class KafkaReactorR2dbcAutoConfigurationTest {
    @Test
    void receiverOptions_builds() {
        KafkaReactorR2dbcAutoConfiguration cfg = new KafkaReactorR2dbcAutoConfiguration();
        ReceiverOptions<String, String> opts = cfg.receiverOptions("localhost:9092", "g", "topic1");
        assertThat(opts).isNotNull();
    }
}
