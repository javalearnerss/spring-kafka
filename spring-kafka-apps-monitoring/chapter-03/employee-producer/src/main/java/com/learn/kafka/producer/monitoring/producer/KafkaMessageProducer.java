package com.learn.kafka.producer.monitoring.producer;

import com.learn.kafka.producer.monitoring.metrics.KafkaProducerMetricsService;
import com.learn.kafka.producer.monitoring.utils.KafkaCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for sending messages to Kafka topics and emitting metrics for success and failure scenarios.
 * It uses KafkaTemplate to send messages and KafkaProducerMetricsService to emit metrics with appropriate tags
 */
@Service
public class KafkaMessageProducer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);

    @Value("${producer.name}")
    private String producerName;

    @Value("${spring.application.name}")
    private String application;

    private final KafkaTemplate<K, V> kafkaTemplate;
    private final KafkaProducerMetricsService producerMetrics;

    public KafkaMessageProducer(KafkaTemplate<K, V> kafkaTemplate, KafkaProducerMetricsService producerMetrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.producerMetrics = producerMetrics;
    }

    /**
     * Sends a message to the specified Kafka topic with the given key and value.
     * @param topic the Kafka topic to which the message will be sent
     * @param key  the key associated with the message, used for partitioning
     * @param message the value of the message to be sent to Kafka
     */
    public void send(String topic, K key, V message) {
        producerMetrics.increment(application, producerName, topic, "sent");
        CompletableFuture<SendResult<K, V>> response = kafkaTemplate.send(topic, key, message);
        new KafkaCallbackHandler<>(response)
                .onSuccess(result -> {
                    producerMetrics.increment(application, producerName, topic, "success");
                    LOGGER.info("Event acknowledged : success, topic={} partition={} offset={} key={} payload={}",
                            result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), key, message);
                })
                .onFailure(ex -> {
                    producerMetrics.increment(application, producerName, topic, "failure");
                    LOGGER.error("Event acknowledged : failure, Unable to send message topic={} key={} payload={} due to {}", topic, key, message, ex.toString());
                });
    }


}