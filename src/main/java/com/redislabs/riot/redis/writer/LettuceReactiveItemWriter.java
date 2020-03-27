package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import lombok.Builder;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

public class LettuceReactiveItemWriter<O> extends AbstractLettuceItemWriter<O> {

	@SuppressWarnings("rawtypes")
	@Builder
	protected LettuceReactiveItemWriter(CommandWriter<O> writer,
			GenericObjectPool<? extends StatefulConnection<String, String>> pool, Function api) {
		super(writer, pool, api);
	}

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
