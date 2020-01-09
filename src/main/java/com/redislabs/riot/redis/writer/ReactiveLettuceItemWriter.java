package com.redislabs.riot.redis.writer;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

public class ReactiveLettuceItemWriter<C extends StatefulConnection<String, String>, R extends BaseRedisReactiveCommands<String, String>, O>
		extends AbstractLettuceItemWriter<C, R, O> {

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
