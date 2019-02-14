
package com.redislabs.recharge.redis.writers;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractRedisWriter<T extends AbstractRedisConfiguration>
		extends AbstractItemStreamItemWriter<Map> {

	public static final String KEY_SEPARATOR = ":";

	T config;
	ConversionService converter = new DefaultConversionService();
	private GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;

	protected AbstractRedisWriter(T config) {
		this.config = config;
	}

	protected StatefulRediSearchConnection<String, String> getConnection() throws Exception {
		return pool.borrowObject();
	}

	protected void release(StatefulRediSearchConnection<String, String> connection) {
		pool.returnObject(connection);
	}

	public AbstractRedisWriter<T> setConnectionPool(
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		this.pool = pool;
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

	protected String getKey(Map record) {
		String id = getValues(record, config.getKeys());
		return getKey(config.getKeyspace(), id);
	}

	protected void convert(Map record) {
		for (Object key : record.keySet()) {
			Object value = record.get(key);
			record.put(key, converter.convert(value, String.class));
		}
	}

	protected String getKey(String keyspace, String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

}
