package com.redislabs.riot.redis.writer;

import java.util.List;

import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

public class ReactiveLettuceItemWriter<O> extends AbstractLettuceItemWriter<O> {

	@SuppressWarnings("unchecked")
	@Override
	protected void write(List<? extends O> items, Object commands) {
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
