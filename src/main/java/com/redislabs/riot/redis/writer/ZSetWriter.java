package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

public class ZSetWriter extends AbstractCollectionRedisItemWriter {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		double score = score(item);
		return commands.zadd(redis, key(item), score, member(item));
	}

	private double score(Map<String, Object> item) {
		return converter.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
