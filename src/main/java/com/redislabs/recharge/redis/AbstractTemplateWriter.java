
package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractTemplateWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private StringRedisTemplate template;

	protected AbstractTemplateWriter(StringRedisTemplate template) {
		this.template = template;
	}

	@Override
	public void write(List<? extends Map<String, Object>> records) {
		try {
			template.executePipelined(new RedisCallback<Object>() {
				public Object doInRedis(RedisConnection connection) throws DataAccessException {
					StringRedisConnection conn = (StringRedisConnection) connection;
					for (Map<String, Object> record : records) {
						write(conn, record);
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

	protected abstract void write(StringRedisConnection conn, Map<String, Object> record);

}
