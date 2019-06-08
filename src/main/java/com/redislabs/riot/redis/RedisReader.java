
package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.util.Assert;

import com.redislabs.riot.AbstractReader;

import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisReader extends AbstractReader {

	@Setter
	private JedisPool jedisPool;
	@Setter
	private Integer count;
	@Setter
	private String match;
	@Setter
	private String separator;
	@Setter
	private String keyspace;
	@Setter
	private String[] keys;
	private volatile boolean initialized = false;
	private Object lock = new Object();
	private RedisKeyIterator redisIterator;
	private Jedis jedis;

	@Override
	protected void doOpen() throws Exception {
		Assert.state(!initialized, "Cannot open an already open ItemReader, call close first");
		jedis = jedisPool.getResource();
		redisIterator = new RedisKeyIterator(jedis, count, match);
		initialized = true;
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			redisIterator = null;
			jedisPool.close();
			initialized = false;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Map<String, Object> doRead() throws Exception {
		synchronized (lock) {
			if (redisIterator.hasNext()) {
				String key = redisIterator.next();
				Map map = (Map) jedis.hgetAll(key);
				if (keys.length > 0) {
					String[] keyValues = key.split(separator);
					for (int index = 0; index < keys.length; index++) {
						String keyName = keys[index];
						String keyValue = keyValues[keyspace == null ? index : index + 1];
						map.put(keyName, keyValue);
					}
				}
				return map;
			}
		}
		return null;
	}

}
