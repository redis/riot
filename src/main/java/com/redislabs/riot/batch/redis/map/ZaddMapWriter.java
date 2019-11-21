package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class ZaddMapWriter<R> extends CollectionMapWriter<R> {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	@Override
	protected Object write(R redis, String key, String member, Map<String, Object> item) {
		Double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return commands.zadd(redis, key, score, member);
	}

}
