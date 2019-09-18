package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.Pool;

public class JedisItemWriter extends AbstractRedisItemWriter {

	private final Logger log = LoggerFactory.getLogger(JedisItemWriter.class);

	private Pool<Jedis> pool;
	private RedisMapWriter writer;

	public JedisItemWriter(Pool<Jedis> pool, RedisMapWriter writer) {
		setName(ClassUtils.getShortName(JedisItemWriter.class));
		this.pool = pool;
		this.writer = writer;
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			List<Response<?>> responses = new ArrayList<>();
			for (Map<String, Object> item : items) {
				responses.add(writer.write(p, item));
			}
			p.sync();
			for (Response<?> response : responses) {
				try {
					response.get();
				} catch (Exception e) {
					log.error("Error during write", e);
				}
			}
		}
	}

	@Override
	public synchronized void close() {
		if (pool != null && !hasActiveThreads()) {
			pool.close();
			pool = null;
		}
		super.close();
	}
}
