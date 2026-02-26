package net.rsworld.superduper.consumer.reactive;

import java.util.HashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ReactiveMessageIngestRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@AutoConfiguration
@EnableKafka
@ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "reactor")
public class KafkaReactiveR2dbcAutoConfiguration {

    @Bean
    public ConsumerFactory<String, String> reactiveConsumerFactory(
            @Value("${superduper.kafka.bootstrap-servers}") String bootstrap,
            @Value("${superduper.kafka.group-id}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> reactiveKafkaListenerContainerFactory(
            ConsumerFactory<String, String> cf) {
        var f = new ConcurrentKafkaListenerContainerFactory<String, String>();
        f.setConsumerFactory(cf);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return f;
    }

    @Bean
    KafkaReactiveR2dbcConsumerService kafkaReactorR2dbcConsumerService(
            ReactiveMessageIngestRepository messageIngestRepository,
            org.springframework.beans.factory.ObjectProvider<SuperduperObserver> observerProvider) {
        return new KafkaReactiveR2dbcConsumerService(
                messageIngestRepository, observerProvider.getIfAvailable(() -> NoopSuperduperObserver.INSTANCE));
    }
}
