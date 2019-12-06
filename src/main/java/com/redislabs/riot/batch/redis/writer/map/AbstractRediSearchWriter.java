package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractRediSearchWriter<R> extends AbstractKeyMapRedisWriter<R> {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;

	protected double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
