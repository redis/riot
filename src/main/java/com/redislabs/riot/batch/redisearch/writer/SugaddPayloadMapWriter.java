package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

public class SugaddPayloadMapWriter extends SugaddMapWriter {

	private String payloadField;

	public void setPayloadField(String payloadField) {
		this.payloadField = payloadField;
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> sugadd(RediSearchAsyncCommands<String, String> commands, String index, String string,
			double score, boolean increment, Map<String, Object> item) {
		return commands.sugadd(index, string, score, increment, payload(item));
	}

}
