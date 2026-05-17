package com.learn.kafka.producer.monitoring.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service responsible for managing Kafka producer metrics using Micrometer.
 * It maintains a cache of Counter instances for different combinations of application, producer name, topic, and status.
 * Metrics are emitted with tags to allow for detailed analysis in monitoring systems.
 */
@Service
public class KafkaProducerMetricsService {

    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<KafkaMetricTag, Counter> counterCache = new ConcurrentHashMap<>();

    public KafkaProducerMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Increments the counter for a specific combination of application, producer name, topic, and status.
     *
     * @param application  the name of the application emitting the metric
     * @param producerName producer name associated with the metric
     * @param topic        the Kafka topic associated with the metric
     * @param status       the status of the processing (e.g., "received", "success", "failure")
     */
    public void increment(String application, String producerName, String topic, String status) {
        KafkaMetricTag metricTag = new KafkaMetricTag(application, producerName, topic, status);
        Counter counter = counterCache.computeIfAbsent(metricTag, this::buildCounter);
        counter.increment();
    }

    private Counter buildCounter(KafkaMetricTag tag) {

        return Counter.builder("kafka.producer.messages")
                .tag("application", tag.service())
                .tag("producer_name", tag.group())
                .tag("topic", tag.topic())
                .tag("status", tag.status())
                .register(meterRegistry);
    }
}