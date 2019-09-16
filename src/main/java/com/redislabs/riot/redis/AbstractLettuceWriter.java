package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractLettuceWriter<S extends StatefulConnection<String, String>, C extends BaseRedisAsyncCommands<String, String>>
		extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(AbstractLettuceWriter.class);

	private GenericObjectPoolConfig<S> poolConfig;
	private Supplier<S> supplier;
	private GenericObjectPool<S> pool;
	private LettuceItemWriter<C> writer;

	public AbstractLettuceWriter(GenericObjectPoolConfig<S> poolConfig, Supplier<S> supplier,
			LettuceItemWriter<C> writer) {
		setName(ClassUtils.getShortName(this.getClass()));
		this.poolConfig = poolConfig;
		this.supplier = supplier;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		log.debug("Creating Lettuce pool {}", poolConfig);
		this.pool = ConnectionPoolSupport.createGenericObjectPool(supplier, poolConfig);
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		S connection = pool.borrowObject();
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			C commands = commands(connection);
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				RedisFuture<?> future = writer.write(commands, item);
				if (future != null) {
					futures.add(future);
				}
			}
			commands.flushCommands();
			for (int index = 0; index < futures.size(); index++) {
				RedisFuture<?> future = futures.get(index);
				try {
					future.get(1, TimeUnit.SECONDS);
				} catch (Exception e) {
					log.error("Could not write record {}: {}", items.get(index), future.getError());
				}
			}
		} finally {
			pool.returnObject(connection);
		}
	}

	protected abstract C commands(S connection);

	@Override
	public void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (pool != null && pool.getNumActive() == 0) {
			log.debug("Closing pool");
			pool.close();
			shutdownClient();
			pool = null;
		}
		super.close();
	}

	protected abstract void shutdownClient();

}
