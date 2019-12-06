package com.redislabs.riot.batch.redis.writer;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractLettuceItemWriter<C extends StatefulConnection<String, String>, R, O>
		extends AbstractRedisItemWriter<R, O> {

	@Setter
	private AbstractRedisClient client;
	@Setter
	private Supplier<ClientResources> resources;
	@Setter
	private GenericObjectPool<C> pool;
	@Setter
	private Function api;

	@Override
	public void write(List<? extends O> items) throws Exception {
		C connection = pool.borrowObject();
		R commands = (R) api.apply(connection);
		try {
			write(items, commands);
		} finally {
			pool.returnObject(connection);
		}
	}

	protected abstract void write(List<? extends O> items, R commands);

	@Override
	public void close() {
		if (pool != null) {
			log.debug("Closing Lettuce pool");
			pool.close();
			log.debug("Shutting down Lettuce client");
			client.shutdown();
			log.debug("Shutting down Lettuce client resources");
			resources.get().shutdown();
		}
		super.close();
	}

}
