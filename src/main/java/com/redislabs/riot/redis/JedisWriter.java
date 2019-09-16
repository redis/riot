package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.JedisItemWriter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.Pool;

public class JedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(JedisWriter.class);

	private Pool<Jedis> pool;
	private JedisItemWriter writer;

	public JedisWriter(Pool<Jedis> pool, JedisItemWriter writer) {
		setName(ClassUtils.getShortName(JedisWriter.class));
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
	public void close() {
		pool.close();
		super.close();
	}
}
