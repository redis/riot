
package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.config.KeyConfiguration;

public abstract class AbstractTemplateWriter extends AbstractRedisWriter {

	private StringRedisTemplate template;

	protected AbstractTemplateWriter(KeyConfiguration config, StringRedisTemplate template) {
		super(config);
		this.template = template;
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		template.executePipelined(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				StringRedisConnection conn = (StringRedisConnection) connection;
				for (Map<String, Object> item : items) {
					write(conn, item);
				}
				return null;
			}
		});
	}

	protected abstract void write(StringRedisConnection conn, Map<String, Object> map);

}
