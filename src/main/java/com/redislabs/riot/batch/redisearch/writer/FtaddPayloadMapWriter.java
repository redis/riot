package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings("unchecked")
@Accessors(fluent = true)
public class FtaddPayloadMapWriter<R> extends AbstractSearchMapWriter<R> {

	@Setter
	private String payloadField;

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String payload = convert(item.remove(payloadField), String.class);
		return commands.ftadd(redis, index, key, score(item), stringMap(item), options, payload);
	}

}
