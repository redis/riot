package com.redislabs.riot.batch.redis;

import java.util.List;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import lombok.extern.slf4j.Slf4j;
import reactor.core.CorePublisher;

@Slf4j
public class LettuceReactiveItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisReactiveCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	private RedisWriter<R, O> writer;

	public LettuceReactiveItemWriter(LettuceConnector<K, V, C, R> connector, RedisWriter<R, O> writer) {
		super(connector);
		this.writer = writer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void write(List<? extends O> items, R commands) {
		for (O item : items) {
			CorePublisher publisher = (CorePublisher) writer.write(commands, item);
			if (publisher == null) {
				continue;
			}
			publisher.subscribe(new Subscriber() {

				@Override
				public void onComplete() {
					// do nothing
				}

				@Override
				public void onError(Throwable t) {
					if (log.isDebugEnabled()) {
						log.debug("Could not write record {}", item, t);
					} else {
						log.error("Could not write record: {}", t.getMessage());
					}
				}

				public void onNext(Object t) {
				}

				@Override
				public void onSubscribe(Subscription s) {
				}

			});
		}
	}

}
