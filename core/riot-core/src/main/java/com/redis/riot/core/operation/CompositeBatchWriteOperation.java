package com.redis.riot.core.operation;

import java.util.List;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeBatchWriteOperation<K, V, T> implements Operation<K, V, T, Object> {

	private final List<Operation<K, V, T, Object>> delegates;

	public CompositeBatchWriteOperation(List<Operation<K, V, T, Object>> delegates) {
		this.delegates = delegates;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends T> inputs,
			Chunk<RedisFuture<Object>> outputs) {
		for (Operation<K, V, T, Object> delegate : delegates) {
			delegate.execute(commands, inputs, outputs);
		}
	}

}
