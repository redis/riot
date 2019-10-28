package com.redislabs.riot.batch.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.redis.writer.RedisMapWriter;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.Pool;

@Slf4j
public class JedisItemWriter extends AbstractRedisItemWriter {

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
