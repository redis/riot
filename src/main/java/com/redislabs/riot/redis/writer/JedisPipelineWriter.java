package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class JedisPipelineWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	@Setter
	private JedisPool pool;
	@Setter
	private AbstractRedisItemWriter itemWriter;

	public JedisPipelineWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	public String getName() {
		return getExecutionContextKey("name");
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			for (Map<String, Object> item : items) {
				itemWriter.write(p, item);
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
