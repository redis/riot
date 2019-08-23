package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class LettuceWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(LettuceWriter.class);

	private AbstractLettuceConnector connector;
	private LettuceItemWriter writer;

	public LettuceWriter(AbstractLettuceConnector connector, LettuceItemWriter writer) {
		setName(ClassUtils.getShortName(LettuceWriter.class));
		this.connector = connector;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		connector.open();
		super.open(executionContext);
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
		if (connector.isClosed()) {
			return;
		}
		StatefulRedisConnection<String, String> connection = connector.borrowConnection();
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
			try {
				boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
						futures.toArray(new RedisFuture[futures.size()]));
				if (result) {
					log.debug("Wrote {} records", items.size());
				} else {
					log.warn("Could not write {} records", items.size());
					for (RedisFuture<?> future : futures) {
						if (future.getError() != null) {
							log.error(future.getError());
						}
					}
				}
			} catch (RedisCommandExecutionException e) {
				log.error("Could not execute commands", e);
			}
		} finally {
			connector.returnConnection(connection);
		}
	}

	@Override
	public void close() {
		connector.close();
		super.close();
	}

	@Override
	public String toString() {
		return writer.toString();
	}

}
