package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class SugaddPayloadMapWriter<R> extends SugaddMapWriter<R> {

	@Setter
	private String payloadField;

	@Override
	protected Object write(R redis, String key, Map<String, Object> item, String string, double score) {
		return commands.sugadd(redis, index, string, score, increment, payload(item));
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return convert(item.remove(payloadField), String.class);
	}

}
