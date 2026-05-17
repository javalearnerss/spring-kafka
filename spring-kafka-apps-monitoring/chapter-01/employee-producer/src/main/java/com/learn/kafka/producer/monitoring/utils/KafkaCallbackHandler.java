package com.learn.kafka.producer.monitoring.utils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class KafkaCallbackHandler<T> {
    private final CompletableFuture<T> completableFuture;

    public KafkaCallbackHandler(CompletableFuture<T> result) {
        this.completableFuture = result;
    }

    public KafkaCallbackHandler<T> onSuccess(Consumer<T> success) {
        completableFuture.thenAccept(success);
        return this;
    }

    public KafkaCallbackHandler<T> onFailure(Consumer<Throwable> failure) {
        completableFuture.exceptionally(ex -> {
            failure.accept(ex);
            return null;
        });
        return this;
    }

}
