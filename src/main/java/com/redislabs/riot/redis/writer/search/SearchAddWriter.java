package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

import lombok.Setter;

@Setter
public class SearchAddWriter extends AbstractRediSearchItemWriter {

	@Setter
	private String scoreField;
	@Setter
	private String payloadField;
	@Setter
	private double defaultScore;
	@Setter
	private AddOptions options;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		double score = converter.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		String id = converter.key(item);
		String payload = payload(item);
		return commands.ftadd(redis, index, id, score, item, options, payload);

	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return converter.convert(item.get(payloadField), String.class);
	}

}
