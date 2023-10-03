package com.redis.riot.core.operation;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeBatchWriteOperation<K, V, T> implements BatchWriteOperation<K, V, T> {

    private final List<BatchWriteOperation<K, V, T>> delegates;

    public CompositeBatchWriteOperation(List<BatchWriteOperation<K, V, T>> delegates) {
        this.delegates = delegates;
    }

    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        for (BatchWriteOperation<K, V, T> delegate : delegates) {
            futures.addAll(delegate.execute(commands, items));
        }
        return futures;
    }

}
