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
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractLettuceWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(AbstractLettuceWriter.class);

	private GenericObjectPoolConfig<? extends StatefulRedisConnection<String, String>> poolConfig;
	private Supplier<StatefulRedisConnection<String, String>> supplier;
	private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
	private LettuceItemWriter writer;

	public AbstractLettuceWriter(GenericObjectPoolConfig<? extends StatefulRedisConnection<String, String>> poolConfig,
			Supplier<StatefulRedisConnection<String, String>> supplier, LettuceItemWriter writer) {
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
		StatefulRedisConnection<String, String> connection = pool.borrowObject();
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			RedisAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				RedisFuture<?> future = writer.write(commands, item);
				if (future != null) {
					futures.add(future);
				}
			}
			commands.flushCommands();
			futures.forEach(f -> {
				try {
					f.get(1, TimeUnit.SECONDS);
				} catch (Exception e) {
					log.error("Could not write record: {}", f.getError());
				}
			});
		} finally {
			pool.returnObject(connection);
		}
	}

	@Override
	public void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (pool.getNumActive() == 0) {
			log.debug("Closing pool");
			pool.close();
			shutdownClient();
		}
		super.close();
	}

	protected abstract void shutdownClient();

}
