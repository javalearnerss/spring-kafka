package com.learn.kafka.consumer.monitoring.consumer;

import com.learn.kafka.consumer.monitoring.consumer.exception.RetryException;
import com.learn.kafka.consumer.monitoring.service.EmployeeProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.support.KafkaHeaders.OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;

@Component
public class EmployeeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeConsumer.class);

    private final EmployeeProcessingService processingService;

    public EmployeeConsumer(EmployeeProcessingService processingService) {
        this.processingService = processingService;
    }

    @RetryableTopic(include = RetryException.class, attempts = "${retry.max.attempts}",
            backoff = @Backoff(delayExpression = "${retry.backoff}", multiplierExpression = "${retry.backoff.multiplier}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE, autoCreateTopics = "false", dltTopicSuffix = "-dlt")
    @KafkaListener(topics = "${topic.name}", groupId = "${consumer.group}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload String payload, @Header(RECEIVED_TOPIC) String topic, @Header(OFFSET) Long offset, Acknowledgment ack) throws Exception {

        LOGGER.info("Message received topic={}, offset={}, payload={}", topic, offset, payload);
        processingService.process(payload, topic);
        ack.acknowledge();
        LOGGER.info("Message acknowledged topic={}, offset={}", topic, offset);
    }
}