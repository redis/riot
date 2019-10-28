package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("unchecked")
public class FtaddPayloadMapWriter extends AbstractSearchMapWriter {

	private String payloadField;

	public void setPayloadField(String payloadField) {
		this.payloadField = payloadField;
	}

	private String payload(Map<String, Object> item) {
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

}
