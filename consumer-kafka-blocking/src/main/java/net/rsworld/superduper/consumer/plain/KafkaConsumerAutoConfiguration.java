package net.rsworld.superduper.consumer.plain;

import java.util.HashMap;
import java.util.Map;
import net.rsworld.superduper.observability.api.NoopSuperduperObserver;
import net.rsworld.superduper.observability.api.SuperduperObserver;
import net.rsworld.superduper.repository.api.ConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.DefaultConsumerMetadataResolver;
import net.rsworld.superduper.repository.api.MessageIngestRepository;
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
@ConditionalOnProperty(name = "superduper.consumer.type", havingValue = "spring", matchIfMissing = true)
public class KafkaConsumerAutoConfiguration {
    @Bean
    public ConsumerFactory<String, String> consumerFactory(
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
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> cf) {
        var f = new ConcurrentKafkaListenerContainerFactory<String, String>();
        f.setConsumerFactory(cf);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return f;
    }

    @Bean
    KafkaConsumerService kafkaConsumerService(
            MessageIngestRepository messageIngestRepository,
            org.springframework.beans.factory.ObjectProvider<SuperduperObserver> observerProvider,
            org.springframework.beans.factory.ObjectProvider<ConsumerMetadataResolver> metadataResolverProvider) {
        return new KafkaConsumerService(
                messageIngestRepository,
                observerProvider.getIfAvailable(() -> NoopSuperduperObserver.INSTANCE),
                metadataResolverProvider.getIfAvailable(DefaultConsumerMetadataResolver::new));
    }
}
