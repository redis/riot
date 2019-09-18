
package com.redislabs.riot.redis.reader;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class RedisItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private Integer count;
	private String match;
	private String separator;
	private String keyspace;
	private String[] keys;
	private Pool<Jedis> jedisPool;
	private volatile boolean initialized = false;
	private Object lock = new Object();
	private RedisKeyIterator redisIterator;
	private Jedis jedis;

	public RedisItemReader(Pool<Jedis> jedisPool) {
		setName(ClassUtils.getShortName(RedisItemReader.class));
		this.jedisPool = jedisPool;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setKeys(String[] keys) {
		this.keys = keys;
	}

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
			jedis.close();
			jedisPool.close();
			jedis = null;
			jedisPool = null;
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
