package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class LettuSearchSuggestWriter extends AbstractLettuSearchItemWriter {

	private String field;
	private String scoreField;
	private double defaultScore = 1d;
	private boolean increment;
	private String payloadField;

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		String string = convert(item.get(field), String.class);
		if (string == null) {
			return null;
		}
		double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return commands.sugadd(index, string, score, increment, payload(item));
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return convert(item.remove(payloadField), String.class);
	}

}
