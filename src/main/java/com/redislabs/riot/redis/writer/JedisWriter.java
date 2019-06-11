package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@Slf4j
public class JedisWriter extends AbstractRedisWriter {

	private JedisPool pool;
	private JedisItemWriter writer;

	public JedisWriter(JedisPool pool, JedisItemWriter writer) {
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
