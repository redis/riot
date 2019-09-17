package com.redislabs.riot.redisearch;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.riot.redis.writer.RedisMapWriter;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class AbstractLettuSearchMapWriter
		extends RedisMapWriter<RediSearchAsyncCommands<String, String>> {

	private String index;
	private String scoreField;
	private double defaultScore = 1d;

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public void setDefaultScore(double defaultScore) {
		this.defaultScore = defaultScore;
	}

	public void setScoreField(String scoreField) {
		this.scoreField = scoreField;
	}

	protected double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	public RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write(commands, index, item);
	}

	protected abstract RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item);

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		throw new UnsupportedOperationException("Jedis not supported for RediSearch module");
	}

	@Override
	public void write(JedisCluster cluster, Map<String, Object> item) {
		throw new UnsupportedOperationException("Jedis not supported for RediSearch module");
	}

}
