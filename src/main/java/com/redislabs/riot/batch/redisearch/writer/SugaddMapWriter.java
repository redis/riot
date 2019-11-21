package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class SugaddMapWriter<R> extends AbstractLettuSearchMapWriter<R> {

	@Setter
	private String field;
	@Setter
	protected boolean increment;

	private String string(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String string = string(item);
		if (string == null) {
			return null;
		}
		double score = score(item);
		return write(redis, key, item, string, score);
	}

	@SuppressWarnings("unused")
	protected Object write(R redis, String key, Map<String, Object> item, String string, double score) {
		return commands.sugadd(redis, index, string, score, increment);
	}

	@Override
	public String toString() {
		return String.format("RediSearch suggestion index %s", index);
	}

}
