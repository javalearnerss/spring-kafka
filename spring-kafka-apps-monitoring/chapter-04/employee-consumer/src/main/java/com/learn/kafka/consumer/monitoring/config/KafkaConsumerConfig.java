package com.learn.kafka.consumer.monitoring.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.application.name:employee-service}")
    private String serviceName;

    private final KafkaProperties kafkaProperties;

    private final Environment env;

    public KafkaConsumerConfig(Environment env, KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.env = env;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());

        // Kafka broker addresses
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getProperty("spring.kafka.bootstrap-servers"));

        // Consumer group
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                env.getProperty("consumer.group"));

        // Manual offset commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                Boolean.parseBoolean(env.getProperty("spring.kafka.consumer.enable-auto-commit", "false")));

        // Key deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        // Value deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        // Max records per poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                Integer.parseInt(env.getProperty("spring.kafka.consumer.max-poll-records", "10")));

        // Max poll interval
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                Integer.parseInt(env.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms", "300000")));

        // Session timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                Integer.parseInt(env.getProperty("spring.kafka.consumer.properties.session.timeout.ms", "60000")));

        // Heartbeat interval
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                Integer.parseInt(env.getProperty("spring.kafka.consumer.properties.heartbeat.interval.ms", "20000")));

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setConcurrency(3);

        return factory;
    }



}
