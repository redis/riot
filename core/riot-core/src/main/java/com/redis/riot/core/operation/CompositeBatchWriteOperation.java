package com.redis.riot.core.operation;

import java.util.List;

import com.redis.spring.batch.operation.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeBatchWriteOperation<K, V, T> implements Operation<K, V, T, Object> {

	private final List<Operation<K, V, T, Object>> delegates;

	public CompositeBatchWriteOperation(List<Operation<K, V, T, Object>> delegates) {
		this.delegates = delegates;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> inputs,
			List<RedisFuture<Object>> outputs) {
		for (Operation<K, V, T, Object> delegate : delegates) {
			delegate.execute(commands, inputs, outputs);
		}
	}

}
