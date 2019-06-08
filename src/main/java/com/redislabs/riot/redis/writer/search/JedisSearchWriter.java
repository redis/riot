package com.redislabs.riot.redis.writer.search;

import java.util.List;
import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import io.redisearch.Document;
import io.redisearch.client.AddOptions;
import io.redisearch.client.Client;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JedisSearchWriter extends AbstractRedisWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	private String scoreField;
	@Setter
	private String payloadField;
	@Setter
	private float defaultScore;
	@Setter
	private Client client;
	@Setter
	private AddOptions options;

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		Document[] docs = items.stream().map(item -> document(item)).toArray(Document[]::new);
		boolean[] result = client.addDocuments(options, docs);
		for (int index = 0; index < result.length; index++) {
			if (!result[index]) {
				log.error("Could not add document {}", items.get(index));
			}
		}
	}

	private Document document(Map<String, Object> item) {
		Float score = converter.convert(item.getOrDefault(scoreField, defaultScore), Float.class);
		byte[] payload = payload(item);
		return new Document(converter.key(item), item, score, payload);
	}

	private byte[] payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		String payload = converter.convert(item.get(payloadField), String.class);
		if (payload == null) {
			return null;
		}
		return payload.getBytes();
	}

}
