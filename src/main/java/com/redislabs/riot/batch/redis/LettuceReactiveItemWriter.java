package com.redislabs.riot.batch.redis;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

public class LettuceReactiveItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisReactiveCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	private RedisWriter<R, O> writer;

	public LettuceReactiveItemWriter(LettuceConnector<K, V, C, R> connector, RedisWriter<R, O> writer) {
		super(connector);
		this.writer = writer;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void write(List<? extends O> items, R commands) {
		Flux.just((O[]) items.toArray()).flatMap(item -> (CorePublisher<?>) writer.write(commands, item)).blockLast();
	}

}
