package com.redislabs.recharge.redis;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.HashItem;

@Component
public class RedisWriter extends AbstractItemStreamItemWriter<HashItem> {

	@Autowired
	private StringRedisTemplate template;

	public void write(List<? extends HashItem> items) throws Exception {
		template.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (HashItem item : items) {
					((StringRedisConnection) connection).hMSet(item.getKey(), item.getValue());
				}
				return null;
			}
		});
	}

}
