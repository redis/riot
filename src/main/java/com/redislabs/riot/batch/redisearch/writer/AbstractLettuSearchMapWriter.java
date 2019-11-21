package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.map.AbstractMapWriter;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractLettuSearchMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	protected String index;
	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;

	protected double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
