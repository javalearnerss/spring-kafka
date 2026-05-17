package com.learn.kafka.producer.monitoring.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Producer Configuration
 * This configuration class is responsible for creating:
 * 1. Kafka Producer Factory
 * 2. Kafka Template used to send messages
 */
@Configuration
public class KafkaProducerConfig {

    @Autowired
    private Environment env;

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * ProducerFactory is responsible for creating Kafka Producer instances with the provided configuration.
     *
     * @return DefaultKafkaProducerFactory
     */
    @Bean
    public DefaultKafkaProducerFactory<String, String> producerFactory() {

        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildProducerProperties());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getProperty("spring.kafka.bootstrap-servers"));

        properties.put(ProducerConfig.ACKS_CONFIG,
                env.getProperty("spring.kafka.producer.acks"));

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,
                env.getProperty("spring.kafka.producer.batch-size"));

        properties.put(ProducerConfig.LINGER_MS_CONFIG,
                env.getProperty("spring.kafka.producer.properties.linger.ms"));

        properties.put(ProducerConfig.RETRIES_CONFIG,
                env.getProperty("spring.kafka.producer.retries"));

        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                env.getProperty("spring.kafka.producer.properties.retry.backoff.ms"));

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                env.getProperty("spring.kafka.producer.key-serializer"));

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                env.getProperty("spring.kafka.producer.value-serializer"));

        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                env.getProperty("spring.kafka.producer.properties.max.in.flight.requests.per.connection"));

        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                env.getProperty("spring.kafka.producer.properties.delivery.timeout.ms"));

        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                env.getProperty("spring.kafka.producer.properties.request.timeout.ms"));

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                env.getProperty("spring.kafka.producer.properties.compression.type"));

        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                env.getProperty("spring.kafka.producer.properties.buffer.memory"));

        properties.put(ProducerConfig.CLIENT_ID_CONFIG,
                env.getProperty("spring.kafka.producer.client-id"));

        /* Create Producer Factory using configured properties */
        return new DefaultKafkaProducerFactory<>(properties);
    }

    /**
     * KafkaTemplate Bean : KafkaTemplate provides high-level abstraction to send messages to Kafka. It internally uses ProducerFactory.
     *
     * @return KafkaTemplate
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}