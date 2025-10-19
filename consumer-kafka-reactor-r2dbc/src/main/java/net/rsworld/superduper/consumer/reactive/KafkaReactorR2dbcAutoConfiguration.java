package net.rsworld.superduper.consumer.reactive;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@AutoConfiguration
@ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
public class KafkaReactorR2dbcAutoConfiguration {

    @Bean
    ReceiverOptions<String, String> receiverOptions(
            @Value("${superduper.kafka.bootstrap-servers}") String bootstrap,
            @Value("${superduper.kafka.group-id}") String groupId,
            @Value("${superduper.kafka.topic}") String topic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false);
        return ReceiverOptions.<String, String>create(props).subscription(List.of(topic));
    }

    @Bean
    KafkaReceiver<String, String> kafkaReceiver(ReceiverOptions<String, String> options) {
        return KafkaReceiver.create(options);
    }

    @Bean
    KafkaReactorR2dbcConsumerService kafkaReactorR2dbcConsumerService(
            KafkaReceiver<String, String> receiver, DatabaseClient db) {
        return new KafkaReactorR2dbcConsumerService(receiver, db);
    }
}
