
package com.redislabs.recharge.redis.writers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisWriterConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractRedisWriter<T extends AbstractRedisWriterConfiguration>
		extends AbstractItemStreamItemWriter<Map> {

	public static final String KEY_SEPARATOR = ":";

	T config;
	StatefulRediSearchConnection<String, String> connection;
	ConversionService converter = new DefaultConversionService();

	protected AbstractRedisWriter(T config) {
		this.config = config;
	}

	public AbstractRedisWriter<T> setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
		return this;
	}

	protected String getValues(Map record, String[] fields) {
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

	@Override
	public void write(List<? extends Map> records) {
		for (Map record : records) {
			String id = getValues(record, config.getKeys());
			String key = getKey(config.getKeyspace(), id);
			write(key, record);
		}
	}

	protected Map convert(Map record) {
		Map map = new HashMap<>();
		record.forEach((k, v) -> map.put(k, converter.convert(v, String.class)));
		return map;
	}

	protected String getKey(String keyspace, String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

	protected abstract void write(String key, Map record);

}
