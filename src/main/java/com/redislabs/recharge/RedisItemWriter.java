package com.redislabs.recharge;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.config.Recharge;

@Component
public class RedisItemWriter extends ItemStreamSupport implements ItemWriter<Map<String, String>> {

	@Autowired
	private Recharge config;

	@Autowired
	private StringRedisTemplate template;

	private String keyPrefix;

	private String[] keyFields;

	private String keySeparator;

	@Override
	public void open(ExecutionContext executionContext) {
		this.keySeparator = config.getRedis().getKeySeparator();
		this.keyPrefix = config.getRedis().getKeyPrefix() + keySeparator;
		this.keyFields = config.getRedis().getKeyFields();
	}

	@Override
	public void write(List<? extends Map<String, String>> items) throws Exception {
		template.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (Map<String, String> item : items) {
					String key = getKey(item);
					((StringRedisConnection) connection).hMSet(key, item);
				}
				return null;
			}
		});
	}

	private String getKey(Map<String, String> item) {
		String key = keyPrefix;
		for (int i = 0; i < keyFields.length - 1; i++) {
			key += item.get(keyFields[i]) + keySeparator;
		}
		key += item.get(keyFields[keyFields.length - 1]);
		return key;
	}
}
