package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ZaddMapWriter extends CollectionMapWriter {

	private String scoreField;
	private double defaultScore;

	public void setScoreField(String scoreField) {
		this.scoreField = scoreField;
	}

	public void setDefaultScore(double defaultScore) {
		this.defaultScore = defaultScore;
	}

	private double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.zadd(key, score(item), member);
	}

	@Override
	protected void write(JedisCluster cluster, String key, String member, Map<String, Object> item) {
		cluster.zadd(key, score(item), member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, String member, Map<String, Object> item) {
		return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key, score(item), member);
	}

}
