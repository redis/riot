package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractSearchMapCommandWriter extends AbstractKeyMapCommandWriter {

	@Setter
	private String score;
	@Setter
	private double defaultScore = 1d;

	protected double getScore(Map<String, Object> item) {
		return convert(item.getOrDefault(score, defaultScore), Double.class);
	}

}
