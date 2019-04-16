package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.lettusearch.suggest.SuggestAddOptions.SuggestAddOptionsBuilder;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class SuggestWriter extends AbstractRediSearchWriter {

	private String field;
	private String scoreField;
	private double defaultScore = 1d;
	private boolean increment;
	private String payloadField;

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		String string = converter.convert(record.get(field), String.class);
		double score = converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
		if (string == null) {
			return null;
		}
		SuggestAddOptionsBuilder options = SuggestAddOptions.builder().increment(increment);
		if (payloadField != null) {
			options.payload(converter.convert(record.get(payloadField), String.class)).build();
		}
		return commands.sugadd(index, string, score, options.build());
	}

}
