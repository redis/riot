package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import lombok.Setter;

public abstract class AbstractRediSearchWriter extends AbstractKeyMapRedisWriter {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;

	protected double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
