package com.learn.kafka.producer.monitoring.metrics;

import java.util.Objects;

public record KafkaMetricTag(String service, String group, String topic, String status) {

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;

        if (!(o instanceof KafkaMetricTag that)) return false;

        return Objects.equals(service, that.service)
                && Objects.equals(group, that.group)
                && Objects.equals(topic, that.topic)
                && Objects.equals(status, that.status);
    }

}
