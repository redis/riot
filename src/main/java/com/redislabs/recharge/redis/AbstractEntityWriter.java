
package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.redis.key.AbstractKeyBuilder;
import com.redislabs.recharge.redis.key.KeyBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEntityWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private StringRedisTemplate template;
	private KeyBuilder keyBuilder;

	protected AbstractEntityWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity) {
		this.template = template;
		this.keyBuilder = AbstractKeyBuilder.getKeyBuilder(entity.getKey(), entity.getValue().getKeys());
	}

	@Override
	public void write(List<? extends Map<String, Object>> records) {
		try {
			template.executePipelined(new RedisCallback<Object>() {
				public Object doInRedis(RedisConnection connection) throws DataAccessException {
					StringRedisConnection conn = (StringRedisConnection) connection;
					for (Map<String, Object> record : records) {
						write(conn, getKey(record), record);
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

	protected String getId(Map<String, Object> record) {
		return keyBuilder.getId(record);
	}

	protected String getKey(Map<String, Object> record) {
		return keyBuilder.getKey(record);
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> record);

}
