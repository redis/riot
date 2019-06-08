package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;
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
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return commands.add(index, key(item), score, stringMap(item), options, payload(item));
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return convert(item.remove(payloadField), String.class);
	}

}
