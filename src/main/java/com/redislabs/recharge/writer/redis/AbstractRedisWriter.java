
package com.redislabs.recharge.writer.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public static final String KEY_SEPARATOR = ":";

	ConversionService converter = new DefaultConversionService();
	RedisWriterConfiguration config;
	private StatefulRediSearchConnection<String, String> connection;
	private RediSearchClient client;

	protected AbstractRedisWriter(RediSearchClient client, RedisWriterConfiguration config) {
		this.client = client;
		this.config = config;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		log.info("Opening {}", getClass().getSimpleName());
		connection = client.connect();
		open(connection);
	}

	protected abstract void open(StatefulRediSearchConnection<String, String> connection);

	@Override
	public synchronized void close() {
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
		Arrays.setAll(values, index -> converter.convert(record.get(fields[index]), String.class));
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
		for (Map<String, Object> record : records) {
			String id = getValues(record, config.getKeys());
			String key = getKey(config.getKeyspace(), id);
			write(key, record);
		}
	}

	protected Map<String, String> convert(Map<String, Object> record) {
		Map<String, String> map = new HashMap<>();
		record.forEach((k, v) -> map.put(k, converter.convert(v, String.class)));
		return map;
	}

	protected String getKey(String keyspace, String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

	protected abstract void write(String key, Map<String, Object> record);

}
