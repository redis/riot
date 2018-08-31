
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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public static final String KEY_SEPARATOR = ":";

	private ConversionService converter = new DefaultConversionService();
	private StringRedisTemplate template;
	private String keyspace;
	private String[] keyFields;

	protected AbstractRedisWriter(StringRedisTemplate template, String keyspace, String[] keyFields) {
		this.template = template;
		this.keyspace = keyspace;
		this.keyFields = keyFields;
	}

	protected String getValues(Map<String, Object> record, String[] fields) {
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> convert(record.get(fields[index]), String.class));
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
						String id = getValues(record, keyFields);
						write(conn, keyspace, id, record);
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

	protected abstract void write(StringRedisConnection conn, String keyspace, String id, Map<String, Object> record);

}
