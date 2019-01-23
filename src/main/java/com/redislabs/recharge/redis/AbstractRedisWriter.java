
package com.redislabs.recharge.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public static final String KEY_SEPARATOR = ":";

	ConversionService converter = new DefaultConversionService();
	RedisWriterConfiguration config;
	StatefulRediSearchConnection<String, String> connection;
	RediSearchAsyncCommands<String, String> commands;

	private RediSearchClient client;

	protected AbstractRedisWriter(RediSearchClient client, RedisWriterConfiguration config) {
		this.client = client;
		this.config = config;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		connection = client.connect();
		commands = connection.async();
		commands.setAutoFlushCommands(false);
		doOpen();
	}

	protected void doOpen() {
	}

	@Override
	public void close() {
		commands = null;
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

	protected String getValues(Map<String, Object> record, String[] fields) {
		if (fields == null) {
			return null;
		}
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> record.get(fields[index]));
		return join(values);
	}

	protected String join(String... values) {
		return String.join(KEY_SEPARATOR, values);
	}

	protected <T> T convert(Object source, Class<T> targetType) {
		return converter.convert(source, targetType);
	}

	@Override
	public void write(List<? extends Map<String, Object>> records) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (Map<String, Object> record : records) {
			String id = getValues(record, config.getKeys());
			String key = getKey(config.getKeyspace(), id);
			write(key, record);
		}
		commands.flushCommands();
		boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS, futures.toArray(new RedisFuture[futures.size()]));
		if (result) {
			log.debug("Wrote {} records", records.size());
		} else {
			log.warn("Could not write {} records", records.size());
			for (RedisFuture<?> future : futures) {
				if (future.getError() != null) {
					log.error(future.getError());
				}
			}
		}
	}

	protected Map<String, String> convert(Map<String, Object> record) {
		Map<String, String> map = new HashMap<>();
		record.forEach((k, v) -> map.put(k, converter.convert(v, String.class)));
		return map;
	}

	private String getKey(String keyspace, String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

	protected abstract void write(String key, Map<String, Object> record);

}
