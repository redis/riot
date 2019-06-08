package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

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
			for (Map<String, Object> item : items) {
				writer.write(p, item);
			}
			p.sync();
		}
	}

	@Override
	public void close() {
		pool.close();
		super.close();
	}
}
