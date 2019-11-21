package com.redislabs.riot.batch.redis;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.Pool;

@Slf4j
public class JedisPipelineWriter<O> extends AbstractRedisItemWriter<O> {

	private Pool<Jedis> pool;
	private RedisWriter<Pipeline, O> writer;

	public JedisPipelineWriter(Pool<Jedis> pool, RedisWriter<Pipeline, O> writer) {
		this.pool = pool;
		this.writer = writer;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void write(List<? extends O> items) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			List<Response> responses = new ArrayList<>();
			for (O item : items) {
				responses.add((Response) writer.write(p, item));
			}
			p.sync();
			for (Response response : responses) {
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
