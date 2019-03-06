
package com.redislabs.recharge.redis;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class RedisWriter<T extends DataStructureConfiguration> extends AbstractItemStreamItemWriter<Map> {

	public static final String KEY_SEPARATOR = ":";

	protected T config;
	protected ConversionService converter = new DefaultConversionService();
	protected GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;

	protected RedisWriter(T config, GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		this.config = config;
		this.pool = pool;
	}

	protected String getValues(Map record, String[] fields) {
		if (fields.length == 0) {
			return null;
		}
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> converter.convert(record.get(fields[index]), String.class));
		return join(values);
	}

	protected String join(String... values) {
		return String.join(KEY_SEPARATOR, values);
	}

	protected void convert(Map record) {
		for (Object key : record.keySet()) {
			Object value = record.get(key);
			record.put(key, converter.convert(value, String.class));
		}
	}

	protected String getKey(String id) {
		if (id == null) {
			return config.getKeyspace();
		}
		return join(config.getKeyspace(), id);
	}

}
