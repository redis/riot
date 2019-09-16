package com.redislabs.riot.redisearch;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

public class SugaddItemWriter extends AbstractLettuSearchItemWriter {

	private String field;
	private boolean increment;

	public void setField(String field) {
		this.field = field;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	private String string(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		String string = string(item);
		if (string == null) {
			return null;
		}
		return sugadd(commands, index, string, score(item), increment, item);
	}

	protected RedisFuture<?> sugadd(RediSearchAsyncCommands<String, String> commands, String index, String string,
			double score, boolean increment, @SuppressWarnings("unused") Map<String, Object> item) {
		return commands.sugadd(index, string, score, increment);
	}

	@Override
	public String toString() {
		return String.format("RediSearch suggestion index %s", getIndex());
	}

}
