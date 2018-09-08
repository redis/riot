
package com.redislabs.recharge.redis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public static final String KEY_SEPARATOR = ":";

	private ConversionService converter = new DefaultConversionService();
	private StringRedisTemplate template;
	private RedisWriterConfiguration config;

	protected AbstractRedisWriter(StringRedisTemplate template, RedisWriterConfiguration config) {
		this.template = template;
		this.config = config;
	}

	protected String getValues(Map<String, Object> record, String[] fields) {
		if (fields == null) {
			return null;
		}
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> convert(record.get(fields[index]), String.class));
		return join(values);
	}

	protected String join(String... values) {
		return String.join(KEY_SEPARATOR, values);
	}

	protected <T> T convert(Object source, Class<T> targetType) {
		return converter.convert(source, targetType);
	}

	protected StringRedisTemplate getTemplate() {
		return template;
	}

	@Override
	public void write(List<? extends Map<String, Object>> records) {
		try {
			template.executePipelined(new RedisCallback<Object>() {
				public Object doInRedis(RedisConnection connection) throws DataAccessException {
					StringRedisConnection conn = (StringRedisConnection) connection;
					for (Map<String, Object> record : records) {
						String id = getValues(record, config.getKeys());
						String key = getKey(config.getKeyspace(), id);
						write(conn, key, record);
					}
					return null;
				}

			});
		} catch (RedisPipelineException e) {
			if (log.isDebugEnabled()) {
				log.debug("Could not write records", e);
			}
			log.warn("Could not write records: {}", e.getMessage());
		}
	}

	private String getKey(String keyspace, String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> record);

}
