package com.learn.kafka.consumer.monitoring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.consumer.monitoring.consumer.exception.RetryException;
import com.learn.kafka.consumer.monitoring.metrics.KafkaConsumerMetricsService;
import com.learn.kafka.consumer.monitoring.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Service responsible for processing Employee messages consumed from Kafka.
 * It simulates both retryable and non-retryable failure scenarios based on employee age.
 * Metrics are emitted for received, successful, and failed processing attempts.
 */
@Service
public class EmployeeProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeProcessingService.class);

    private final ObjectMapper objectMapper;
    private final KafkaConsumerMetricsService metricsService;

    @Value("${spring.application.name}")
    private String application;

    @Value("${consumer.group}")
    private String consumerGroup;

    @Value("${topic.name}")
    private String mainTopic;

    public EmployeeProcessingService(ObjectMapper objectMapper, KafkaConsumerMetricsService metricsService) {
        this.objectMapper = objectMapper;
        this.metricsService = metricsService;
    }

    public void process(String payload, String topic) throws Exception {
        LOGGER.info("Starting employee processing, employee payload={}", payload);

        try {
            // Deserialize incoming Kafka payload into Employee object
            Employee employee = objectMapper.readValue(payload, Employee.class);

            LOGGER.info("Employee deserialized, employee={}", employee);

            // Increment received metric for every consumed message
            incrementReceivedMetric(topic);

            // Simulate retryable failure scenario
            simulateRetryableFailure(employee.age(), topic);

            // Simulate non-retryable validation failure
            simulateNonRetryableFailure(employee.age());

            // Increment success metric after successful processing
            metricsService.increment(application, consumerGroup, topic, "success");

            LOGGER.info("Employee processed successfully employeeId={}, topic={}", employee.empId(), topic);
        } catch (RetryException e) {
            LOGGER.error("Retryable processing failed employee payload={}, topic={}, error={}", payload, topic, e.getMessage());
            throw e; // Rethrow to trigger retry mechanism
        } catch (Exception e) {
            // Increment failure metric for other exceptions
            metricsService.increment(application, consumerGroup, topic, "failure");
            LOGGER.error("Unexpected processing error employee payload={}, topic={}, error={}", payload, topic, e.getMessage());
            throw e;
        }
    }

    private void simulateRetryableFailure(int age, String topic) throws Exception {

        // Retry logic only for employees whose age is divisible by 5
        if (age % 5 != 0) {
            return;
        }

        LOGGER.info("Retryable validation triggered age={}, topic={}", age, topic);

        // Fail initial attempt from main topic
        if (isMainTopic(topic)) {
            LOGGER.error("Simulation - Initial processing failed. Sending message to retry topic age={}, topic={}", age, topic);
            throw new RetryException("Initial retryable failure");
        }

        // Increment retry metrics for messages coming from retry topics
        incrementRetryMetric(topic);

        // Simulate random retry success/failure
        boolean retrySucceeded = ThreadLocalRandom.current().nextBoolean();

        if (!retrySucceeded) {
            LOGGER.error("Simulation - Retry processing failed again age={}, topic={}", age, topic);
            throw new RetryException("Retry attempt failed");
        }

        LOGGER.info("Retry processing succeeded age={}, topic={}", age, topic);
    }

    private void simulateNonRetryableFailure(int age) {

        // Permanent validation failure for ages divisible by 7
        if (age % 7 == 0) {
            LOGGER.error("Simulation - Non-retryable validation failure triggered age={}", age);
            throw new IllegalArgumentException("Non retryable validation failure");
        }
    }

    private boolean isMainTopic(String topic) {
        return mainTopic.equals(topic);
    }

    private void incrementReceivedMetric(String topic) {
        LOGGER.info("Incrementing received metric topic={}", topic);
        metricsService.increment(application, consumerGroup, topic, "received");
    }

    private void incrementRetryMetric(String topic) {
        if (topic.contains("-retry-")) {
            LOGGER.info("Incrementing retry metric currentTopic={}", topic);
            metricsService.increment(application, consumerGroup, mainTopic, "retry");
            metricsService.increment(application, consumerGroup, topic, "retry-topic");
        }
    }
}