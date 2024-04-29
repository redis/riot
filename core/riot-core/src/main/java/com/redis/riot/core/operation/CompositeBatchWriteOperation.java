package com.redis.riot.core.operation;

import java.util.List;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeBatchWriteOperation<K, V, T> implements WriteOperation<K, V, T> {

	private final List<WriteOperation<K, V, T>> delegates;

	public CompositeBatchWriteOperation(List<WriteOperation<K, V, T>> delegates) {
		this.delegates = delegates;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> inputs,
			List<RedisFuture<Object>> outputs) {
		for (WriteOperation<K, V, T> delegate : delegates) {
			delegate.execute(commands, inputs, outputs);
		}
	}

}
