package com.learn.kafka.producer.monitoring.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.producer.monitoring.metrics.KafkaProducerMetricsService;
import com.learn.kafka.producer.monitoring.model.Employee;
import com.learn.kafka.producer.monitoring.producer.KafkaMessageProducer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EmployeeGenerator is a Spring service responsible for generating random Employee events
 * and publishing them to a specified Kafka topic at regular intervals.
 * It uses a GenericKafkaProducer to send messages and an ObjectMapper for JSON serialization.
 * The generator can be started and stopped dynamically, and it logs all operations for monitoring purposes.
 */
@Service
public class EmployeeEventGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeEventGenerator.class);

    private static final String[] NAMES = {"Sandeep", "Rahul", "Ankit", "Priya", "Neha", "Amit"};

    private final KafkaMessageProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    private final AtomicBoolean enabled = new AtomicBoolean(true);

    private final KafkaProducerMetricsService producerMetrics;

    @Value("${topic.name:employee}")
    private String topic;

    @Value("${producer.name}")
    private String producerName;

    @Value("${spring.application.name}")
    private String application;

    public EmployeeEventGenerator(KafkaMessageProducer<String, String> producer, ObjectMapper objectMapper, KafkaProducerMetricsService producerMetrics) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.producerMetrics = producerMetrics;
    }

    public void stop() {
        enabled.set(false);
        LOGGER.info("Kafka producer event generation stopped");
    }

    public void start() {
        enabled.set(true);
        LOGGER.info("Kafka producer event generation started");
    }

    /**
     * Scheduled method that generates and sends Employee events to Kafka at fixed intervals.
     * The interval can be configured via the 'employee.generator.interval.ms' property.
     * The method checks if the generator is enabled before proceeding with event generation and publishing.
     */
    @Scheduled(fixedDelayString = "${employee.generator.interval.ms:3000}")
    public void generateAndSendEmployee() {

        if (!enabled.get()) {
            return;
        }

        Employee employee = generateEmployee();
        try {
            LOGGER.info("Publishing employee event topic={} employee={}", topic, employee);
            simulateFailure(employee);
            producer.send(topic, employee.empId(), objectMapper.writeValueAsString(employee));
            LOGGER.info("Employee event published topic={} employee={}", topic, employee);
        } catch (Exception e) {
            producerMetrics.increment(application, producerName, topic, "sent");
            producerMetrics.increment(application, producerName, topic, "failure");
            LOGGER.error("Failed to publish employee event topic={} employee={} reason={}", topic, employee, e.getMessage(), e);
        }
    }

    private Employee generateEmployee() {
        return new Employee(UUID.randomUUID().toString(), generateName(), generateAge());
    }

    private String generateName() {
        return NAMES[ThreadLocalRandom.current().nextInt(NAMES.length)];
    }

    private int generateAge() {
        return ThreadLocalRandom.current().nextInt(20, 60);
    }

    public boolean isRunning() {
        return enabled.get();
    }

    private void simulateFailure(Employee employee) throws Exception {
        if ("Rahul".equals(employee.name())) {
            throw new RuntimeException("Simulated serialization failure");
        }
    }

    @PreDestroy
    public void shutdown() {
        LOGGER.info("Shutting down Kafka employee event generator");
    }
}