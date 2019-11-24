package com.redislabs.riot.batch.redis;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

public class LettuceReactiveItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisReactiveCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	public LettuceReactiveItemWriter(LettuceConnector<K, V, C, R> connector) {
		super(connector);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void write(List<? extends O> items, R commands) {
		Flux.just((O[]) items.toArray()).flatMap(item -> {
			try {
				return (CorePublisher<?>) writer.write(commands, item);
			} catch (Exception e) {
				logWriteError(item, e);
				return null;
			}
		}).blockLast();
	}

}
