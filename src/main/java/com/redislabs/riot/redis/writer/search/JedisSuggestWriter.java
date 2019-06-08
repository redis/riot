package com.redislabs.riot.redis.writer.search;

import java.util.List;
import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import io.redisearch.Suggestion;
import io.redisearch.client.Client;
import lombok.Setter;

public class JedisSuggestWriter extends AbstractRedisWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	private Client client;
	@Setter
	private String field;
	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;
	@Setter
	private boolean increment;
	@Setter
	private String payloadField;

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		for (Map<String, Object> item : items) {
			String string = converter.convert(item.get(field), String.class);
			if (string == null) {
				continue;
			}
			double score = converter.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
			Suggestion suggestion = Suggestion.builder().payload(payload(item)).score(score).str(string).build();
			client.addSuggestion(suggestion, increment);
		}
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return converter.convert(item.remove(payloadField), String.class);
	}

}
