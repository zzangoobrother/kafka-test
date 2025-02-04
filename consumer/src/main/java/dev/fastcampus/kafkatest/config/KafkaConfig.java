package dev.fastcampus.kafkatest.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVER = "localhost:29092";
    private final static String GROUP_ID = "group-test-2";

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        int i = (int) (1024 * 1024 * 1.6);

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(i));
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "524288");
        configProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(i));

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(5);
        factory.setBatchListener(true);

        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
