package com.learn.kafka.consumer.monitoring.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service responsible for managing Kafka consumer metrics using Micrometer.
 * It maintains a cache of Counter instances for different combinations of application, consumer group, topic, and status.
 * Metrics are emitted with tags to allow for detailed analysis in monitoring systems.
 */
@Service
public class KafkaConsumerMetricsService {

    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<KafkaMetricTag, Counter> counterCache = new ConcurrentHashMap<>();

    public KafkaConsumerMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Increments the counter for a specific combination of application, consumer group, topic, and status.
     *
     * @param application the name of the application emitting the metric
     * @param consumerGroup the consumer group associated with the metric
     * @param topic    the Kafka topic associated with the metric
     * @param status   the status of the processing (e.g., "received", "success", "failure")
     */
    public void increment(String application, String consumerGroup, String topic, String status) {
        KafkaMetricTag metricTag = new KafkaMetricTag(application, consumerGroup, topic, status);
        Counter counter = counterCache.computeIfAbsent(metricTag, this::buildCounter);
        counter.increment();
    }

    private Counter buildCounter(KafkaMetricTag tag) {

        return Counter.builder("kafka.consumer.messages")
                .tag("application", tag.service())
                .tag("consumer_group", tag.group())
                .tag("topic", tag.topic())
                .tag("status", tag.status())
                .register(meterRegistry);
    }
}