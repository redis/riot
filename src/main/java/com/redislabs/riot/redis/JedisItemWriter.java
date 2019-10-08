package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.RedisMapWriter;

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

	@Override
	protected void doWrite(List<? extends Map<String, Object>> items) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			List<Response<?>> responses = new ArrayList<>();
			items.forEach(item -> responses.add(writer.write(p, item)));
			p.sync();
			for (Response<?> response : responses) {
				if (response == null) {
					continue;
				}
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
